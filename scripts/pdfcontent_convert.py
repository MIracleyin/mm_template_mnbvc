from datetime import datetime
from loguru import logger
from pathlib import Path
import argparse
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src.mm_data.core.models.PDFContent_block import pdfcontentjsonl_to_pdf_blocks, batch_to_parquet


def main():
    parser = argparse.ArgumentParser(description="Chinaxiv Convert")
    parser.add_argument("--input_jsonl", "-i", type=Path, help="Input file")
    parser.add_argument("--output_parquet", "-o", type=Path, help="Output file")
    parser.add_argument("--sqlite_path", "-sq", default="pdf_path_map.db", type=Path, help="Sqlite file")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--split_size", "-sp", default=None, type=int,
                       help="Rows per parquet file (optional; used only when --target_size_gb is not provided)")
    group.add_argument("--target_size_gb", "-tsg", default=None, type=float,
                       help="Target parquet file size in GB (preferred default). If omitted and --split_size is also omitted, defaults to 0.9GB.")
    parser.add_argument("--log_dir", "-l", type=Path,
                        default="logs", help="Log level")
    args = parser.parse_args()

    current_date = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")

    input_jsonl = args.input_jsonl
    output_parquet = args.output_parquet
    sqlite_path = args.sqlite_path
    split_size = args.split_size
    target_size_gb = args.target_size_gb

    # Default behavior: prefer target_size_gb; if neither provided, default to 0.9 GB
    if target_size_gb is None and split_size is None:
        target_size_gb = 5.0
        logger.info("No --target_size_gb or --split_size provided; defaulting --target_size_gb to 5.0 GB")

    log_dir = args.log_dir
    log_dir.mkdir(parents=True, exist_ok=True)
    logger_file = log_dir / f"to_mm_{current_date}.log"
    logger.add(logger_file, encoding="utf-8", rotation="500MB")

    # 确保输出目录存在
    if output_parquet is not None and output_parquet.parent:
        output_parquet.parent.mkdir(parents=True, exist_ok=True)

    batchs = pdfcontentjsonl_to_pdf_blocks(input_jsonl, sqlite_path)

    # 将数据流式写入 parquet 文件
    batch_to_parquet(
        output_parquet,
        batchs,
        target_size_gb=target_size_gb,
        split_size=split_size,
    )


if __name__ == "__main__":
    main()