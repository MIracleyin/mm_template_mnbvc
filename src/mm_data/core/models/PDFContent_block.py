import json
import itertools
from pathlib import Path
from typing import Iterable, Iterator, Optional
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as parquet
from loguru import logger
from src.mm_data.core.models.mmdata_block import mmDataBlock
from src.mm_data.core.processor import get_md5
import sqlite3

"""
PDFContent 数据
1. pdf block
    1. 包含 PDF 文件的二进制数据和 PDF 文件的名称, 块类型为 "pdf"
    2. metadata 信息为 {"file_size": 0.0, "file_available": true, "metadata": {}, "timestamp": 0, "language": "english", "num_page": 0, "num_text":0 , "xrefs":[]}
"""
class PDFContentBlock(mmDataBlock):
    """ PDFContent 数据块 """
    def __repr__(self):
        return f"PDFContentBlock(实体ID={self.实体ID}, 块ID={self.块ID}, 块类型={self.块类型}, 时间={self.时间}, 扩展字段={self.扩展字段})"
    


def pdfcontentjsonl_to_pdf_blocks(input_file: Path, sqlite_path: Path) -> Iterator[PDFContentBlock]:
    """按行流式读取 jsonl，逐个生成 PDFContentBlock，避免一次性载入内存。"""
    block_id = 0
    pdfcontent_jsonl_file = input_file
    with pdfcontent_jsonl_file.open("r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            data = json.loads(line)
            md5 = pdfpath2md5(data["file_path"], sqlite_path)
            block_type = "pdf"
            text = "".join(data["text"])
            extend_field = {
                "file_name": data["file_path"].split("/")[-1],
                "file_size": data["file_size"],
                "file_available": data["file_available"],
                "metadata": data["metadata"],
                "timestamp": data["timestamp"],
                "language": data["language"],
                "num_page": len(data["text"]),
                "num_text": len(text.replace("\n", "")),
                "text": data["text"],  # 带分页信息
                "xrefs": data["xref"],
                "toc": data["toc"],
            }
            extend_field_str = json.dumps(extend_field, ensure_ascii=False)

            yield PDFContentBlock(
                实体ID=md5,
                md5=md5,
                块ID=block_id,
                块类型=block_type,
                扩展字段=extend_field_str,
                图片=None,
                文本=text,
            )
            block_id += 1

def pdfpath2md5(pdf_path: str, sqlite_path: Path) -> str:
    """
    将 PDF 计算 md5, 然后将 md5 作为 pdfcontent 文件名，使用 sqlite 保存 md5 和 pdf 文件路径的映射关系
    """
    # 如果 sqlite 文件不存在，则创建并建表
    if not sqlite_path.exists():
        sqlite_path.parent.mkdir(parents=True, exist_ok=True)
        sqlite_path.touch()
        with sqlite3.connect(sqlite_path) as conn:
            cursor = conn.cursor()
            cursor.execute("CREATE TABLE IF NOT EXISTS pdf_path_map (md5 TEXT PRIMARY KEY, pdf_path TEXT)")
            conn.commit()
    # 插入或更新 md5 与 pdf_path 的映射关系
    md5 = get_md5(pdf_path)
    with sqlite3.connect(sqlite_path) as conn:
        cursor = conn.cursor()
        cursor.execute("CREATE TABLE IF NOT EXISTS pdf_path_map (md5 TEXT PRIMARY KEY, pdf_path TEXT)")
        cursor.execute("SELECT 1 FROM pdf_path_map WHERE md5 = ?", (md5,))
        result = cursor.fetchone()
        if result:
            cursor.execute("UPDATE pdf_path_map SET pdf_path = ? WHERE md5 = ?", (pdf_path, md5))
        else:
            cursor.execute("INSERT INTO pdf_path_map (md5, pdf_path) VALUES (?, ?)", (md5, pdf_path))
        conn.commit()
    return md5

def batch_to_parquet(
    output_file: Path,
    blocks: Iterable[PDFContentBlock],
    target_size_gb: Optional[float] = None,
    split_size: Optional[int] = None,
):
    """
    将 blocks 流式写入多个 parquet 文件。

    优先使用 target_size_gb 控制输出大小；当未提供或非正数时，回退使用 split_size（行数）；
    若两者皆未提供，回退至默认行数 10000。

    - 当提供 target_size_gb 时，先用少量样本估算每行平均大小，自动计算每个文件的行数以逼近目标大小。
    - 否则使用 split_size 控制每个文件的最大行数。
    """
    iterator = iter(blocks)

    # 计算每个文件的目标行数
    rows_per_file: Optional[int] = None
    if target_size_gb is not None and target_size_gb > 0:
        # 采样上限默认 1000；如果显式提供 split_size，则不超过它
        sample_limit = 1000 if split_size is None else min(1000, max(1, split_size))
        sample_rows: list[PDFContentBlock] = []
        try:
            for _ in range(sample_limit):
                sample_rows.append(next(iterator))
        except StopIteration:
            pass

        if sample_rows:
            df_sample = pd.DataFrame([row.to_dict() for row in sample_rows])
            table_sample = pa.Table.from_pandas(df_sample)
            sink = pa.BufferOutputStream()
            parquet.write_table(
                table_sample,
                sink,
                compression='zstd',
                row_group_size=100000,
            )
            size_bytes = sink.getvalue().size
            avg_bytes_per_row = max(1, size_bytes // len(sample_rows))
            target_bytes = int(target_size_gb * (1024 ** 3))
            rows_per_file = max(1, target_bytes // avg_bytes_per_row)
            logger.info(
                f"estimated avg_bytes_per_row={avg_bytes_per_row}, rows_per_file≈{rows_per_file} for target {target_size_gb}GB",
            )
        else:
            logger.warning("no sample rows available; fallback to split_size/default rows")

        # 将样本行与剩余迭代器串联起来继续处理
        iterator = itertools.chain(sample_rows, iterator)

    # 如未基于 target_size_gb 计算出行数，则使用 split_size 或默认值
    if rows_per_file is None:
        if split_size is not None and split_size > 0:
            rows_per_file = split_size
        else:
            rows_per_file = 10000

    batch_rows: list[PDFContentBlock] = []
    split_count = 0
    for block in iterator:
        batch_rows.append(block)
        if len(batch_rows) >= rows_per_file:
            df = pd.DataFrame([row.to_dict() for row in batch_rows])
            output_file_split = output_file.parent / f"{output_file.stem}_{split_count}.parquet"
            table = pa.Table.from_pandas(df)
            parquet.write_table(
                table,
                output_file_split,
                compression='zstd',
                row_group_size=100000,
            )
            logger.info(f"batch {split_count} done, {output_file_split} generated")
            batch_rows = []
            split_count += 1

    if batch_rows:
        df = pd.DataFrame([row.to_dict() for row in batch_rows])
        output_file_last = output_file.parent / f"{output_file.stem}_{split_count}.parquet"
        table = pa.Table.from_pandas(df)
        parquet.write_table(
            table,
            output_file_last,
            compression='zstd',
            row_group_size=100000,
        )
        logger.info(f"batch {split_count} done, {output_file_last} generated")

    

