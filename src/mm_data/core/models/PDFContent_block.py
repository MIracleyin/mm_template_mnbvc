import json
from pathlib import Path
from typing import List
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
    


def pdfcontentjsonl_to_pdf_blocks(input_file: Path, sqlite_path: str) -> List[PDFContentBlock]:
    """将 PDF 文件转换为 PDFContentBlock 列表"""
    blocks, block_id = [], 0

    pdfcontent_jsonl_file = input_file

    with pdfcontent_jsonl_file.open("r", encoding="utf-8") as f:
        for line in f:
            data = json.loads(line)
            md5 = pdfpath2md5(data["file_path"], sqlite_path)
            # md5 = "" # 暂时不使用 md5
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
                "text": data["text"], # 带分页信息
                "xrefs": data["xref"],
                "toc": data["toc"],
            }
            extend_field_str = json.dumps(extend_field, ensure_ascii=False)

            blocks.append(PDFContentBlock(
                实体ID=md5,
                md5=md5,
                块ID=block_id,
                块类型=block_type,
                扩展字段=extend_field_str,
                图片=None,
                文本=text,
            ))
            block_id += 1

    return blocks

def pdfpath2md5(pdf_path: str, sqlite_path: str) -> str:
    """
    将 PDF 计算 md5, 然后将 md5 作为 pdfcontent 文件名，使用 sqlite 保存 md5 和 pdf 文件路径的映射关系
    """
    # 如果 sqlite 文件不存在，则创建
    if not sqlite_path.exists():
        sqlite_path.touch()
        conn = sqlite3.connect(sqlite_path)
        cursor = conn.cursor()
        cursor.execute("CREATE TABLE pdf_path_map (md5 TEXT PRIMARY KEY, pdf_path TEXT)")
        conn.commit()
    # 如果 sqlite 文件存在，直接插入 md5 和 pdf 文件路径的映射关系
    md5 = get_md5(pdf_path)
    conn = sqlite3.connect(sqlite_path)
    cursor = conn.cursor()
    # 如果标注中 md5 和 pdf_path 都存在，则更新 pdf_path
    cursor.execute("SELECT * FROM pdf_path_map WHERE md5 = ?", (md5,))
    result = cursor.fetchone()
    if result:
        cursor.execute("UPDATE pdf_path_map SET pdf_path = ? WHERE md5 = ?", (pdf_path, md5))
    else:
        cursor.execute("INSERT INTO pdf_path_map (md5, pdf_path) VALUES (?, ?)", (md5, pdf_path))
    conn.commit()
    return md5

def batch_to_parquet(output_file: Path, split_size: int, blocks: List[PDFContentBlock]):
    # 将 blocks 转换为 parquet 文件
    batch_rows = []
    # 将 batchs 按 split_size 分割，
    # 当 batch 长度大于 split_size 时，将 batch 写入 parquet 文件
    # 当 batch 长度小于 split_size 时，继续追加 batch_rows
    batch_count = 0
    split_count = 0
    for block in blocks:
        batch_count += 1
        batch_rows.append(block)
        if batch_count >= split_size:
            df = pd.DataFrame([row.to_dict() for row in batch_rows])
            output_file_split = output_file.parent / \
                f"{output_file.stem}_{split_count}.parquet"
            # 使用 pyarrow 引擎
            table = pa.Table.from_pandas(df)
            # 保存为 parquet
            parquet.write_table(table,
                                output_file_split,
                                compression='zstd',  # 可选: 'snappy', 'gzip', 'brotli', 'zstd', 'lz4'
                                row_group_size=100000  # 优化读写性能的行组大小
                                )
            logger.info(
                f"batch {split_count} done, {output_file_split} generated")
            batch_rows = []
            batch_count = 0
            split_count += 1
    if batch_rows:
        df = pd.DataFrame([row.to_dict() for row in batch_rows])
        output_file_last = output_file.parent / f"{output_file.stem}_{split_count}.parquet"
        table = pa.Table.from_pandas(df)
        parquet.write_table(table,
                            output_file_last,
                            compression='zstd',  # 可选: 'snappy', 'gzip', 'brotli', 'zstd', 'lz4'
                            row_group_size=100000  # 优化读写性能的行组大小
                            )
        logger.info(
            f"batch {split_count} done, {output_file_last} generated")

    

