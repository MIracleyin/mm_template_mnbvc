import argparse
from datetime import datetime
from pathlib import Path
from loguru import logger
from src.mm_data.core.processor import file_to_blocks, batch_to_parquet


def main():
    parser = argparse.ArgumentParser(description="Docling Convert")
    parser.add_argument("--input_file", "-i", type=Path, help="Input file")
    parser.add_argument("--output_file", "-o", type=Path, help="Output file")
    parser.add_argument("--split_size", "-s", type=int, default=200,
                        help="Split size")  # 500-1000MB 一个 parquet 文件
    parser.add_argument("--log_dir", "-l", type=Path,
                        default="logs", help="Log level")
    args = parser.parse_args()

    current_date = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")

    input_file = args.input_file
    output_file = args.output_file
    split_size = args.split_size

    log_dir = args.log_dir
    logger_file = log_dir / f"to_mm_{current_date}.log"
    logger.add(logger_file, encoding="utf-8", rotation="500MB")

    if input_file.suffix == ".txt":
        input_file_list = input_file.read_text().splitlines()
        input_file_path_list = [input_file.parent /
                                file_path for file_path in input_file_list]
        logger.info(f"input_file_path_list: {input_file_path_list}")
        batchs = [file_to_blocks(input_file)
                  for input_file in input_file_path_list]
    else:
        batchs = [file_to_blocks(input_file)]

    # 将 batch_rows 转换为 jsonl 文件
    batch_to_parquet(output_file, split_size, batchs)


if __name__ == "__main__":
    main()
