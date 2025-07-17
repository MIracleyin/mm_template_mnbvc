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
    parser.add_argument("--split_size", "-sp", default="10000", type=int,
                        help="Split size")  # 500-1000MB 一个 parquet 文件
    parser.add_argument("--log_dir", "-l", type=Path,
                        default="logs", help="Log level")
    args = parser.parse_args()

    current_date = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")

    input_jsonl = args.input_jsonl
    output_parquet = args.output_parquet
    sqlite_path = args.sqlite_path
    split_size = args.split_size

    log_dir = args.log_dir
    logger_file = log_dir / f"to_mm_{current_date}.log"
    logger.add(logger_file, encoding="utf-8", rotation="500MB")

    batchs = pdfcontentjsonl_to_pdf_blocks(input_jsonl, sqlite_path)

    # 将 batch_rows 转换为 parquet 文件
    batch_to_parquet(output_parquet, split_size, batchs)


if __name__ == "__main__":
    main()