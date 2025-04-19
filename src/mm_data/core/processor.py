"""
2025.03.19 miracleyin@live.com
"""

import hashlib
from .models.mmdata_block import mmDataBlock
from pathlib import Path
from typing import List
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as parquet
from loguru import logger

def file_to_blocks(file_path: Path) -> List[mmDataBlock]:
    """将文件转换为 mmDataBlock 列表"""
    # 读取文件
    with open(file_path, 'r') as file:
        # 将文件内容转换为 mmDataBlock 列表
        blocks = [mmDataBlock.from_json(line) for line in file]
    return blocks

def batch_to_parquet(output_file: Path, split_size: int, batchs: List[List[mmDataBlock]]):
    # 将 rows 写入 parquet 文件
    batch_rows = []
    # 将 batchs 按 split_size 分割，
    # 当 batch 长度大于 split_size 时，将 batch 写入 parquet 文件
    # 当 batch 长度小于 split_size 时，继续追加 batch_rows
    batch_count = 0
    split_count = 0
    for batch in batchs:
        batch_count += 1
        batch_rows.extend(batch)
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

    # 处理最后一个 batch
    if batch_rows:
        df = pd.DataFrame([row.to_dict() for row in batch_rows])
        output_file_last = output_file.parent / \
            f"{output_file.stem}_{split_count}.parquet"
        table = pa.Table.from_pandas(df)
        parquet.write_table(table,
                            output_file_last,
                            compression='zstd',  # 可选: 'snappy', 'gzip', 'brotli', 'zstd', 'lz4'
                            row_group_size=100000  # 优化读写性能的行组大小
                            )
        logger.info(f"batch {split_count} done, {output_file_last} generated")

def get_md5(text: str) -> str:
    """获取文本的md5值"""
    return hashlib.md5(text.encode('utf-8')).hexdigest()
