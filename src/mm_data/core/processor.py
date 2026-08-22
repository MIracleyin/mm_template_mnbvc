"""
2025.03.19 miracleyin@live.com
"""

import hashlib
from .models.mmdata_block import blocks_to_table, mmDataBlock
from pathlib import Path
from typing import Iterable, List
import pyarrow.parquet as parquet
from loguru import logger

#: 与 batch_to_parquet 原有行为一致
COMPRESSION = "zstd"
ROW_GROUP_SIZE = 100000


def file_to_blocks(file_path: Path) -> List[mmDataBlock]:
    """将文件转换为 mmDataBlock 列表"""
    # 读取文件
    with open(file_path, 'r') as file:
        # 将文件内容转换为 mmDataBlock 列表
        blocks = [mmDataBlock.from_json(line) for line in file]
    return blocks


def write_blocks(blocks: Iterable[mmDataBlock], output_file: Path) -> None:
    """按 BLOCK_SCHEMA 把块写成一个 parquet 文件。

    所有写出路径统一走这里。原来有三处各写各的
    （chinaxiv_block.batch_to_parquet、processor.batch_to_parquet、
    video_block.block_to_parquet），且都用 pandas 推断类型——
    见 mmdata_block.BLOCK_SCHEMA 里的说明。
    """
    output_file = Path(output_file)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    parquet.write_table(
        blocks_to_table(list(blocks)),
        output_file,
        compression=COMPRESSION,
        row_group_size=ROW_GROUP_SIZE,
    )


def batch_to_parquet(output_file: Path, split_size: int, batchs: List[List[mmDataBlock]]):
    """按 split_size 个 batch 一个文件写出"""
    batch_rows: List[mmDataBlock] = []
    batch_count = 0
    split_count = 0
    for batch in batchs:
        batch_count += 1
        batch_rows.extend(batch)
        if batch_count >= split_size:
            output_file_split = output_file.parent / \
                f"{output_file.stem}_{split_count}.parquet"
            write_blocks(batch_rows, output_file_split)
            logger.info(
                f"batch {split_count} done, {output_file_split} generated")
            batch_rows = []
            batch_count = 0
            split_count += 1

    # 处理最后一个 batch
    if batch_rows:
        output_file_last = output_file.parent / \
            f"{output_file.stem}_{split_count}.parquet"
        write_blocks(batch_rows, output_file_last)
        logger.info(f"batch {split_count} done, {output_file_last} generated")


def get_md5(text: str) -> str:
    """获取文本的md5值"""
    return hashlib.md5(text.encode('utf-8')).hexdigest()
