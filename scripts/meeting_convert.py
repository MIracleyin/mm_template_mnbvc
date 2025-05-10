import sys
import os
from datetime import datetime
from typing import List

from loguru import logger
from pathlib import Path
import argparse

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src.mm_data.core.models.video_block import process_video_to_parquets


def main():
    parser = argparse.ArgumentParser(description="Meeting Vedio Convert")
    parser.add_argument("--input_dir", "-i", type=Path, help="Input files directory")
    parser.add_argument("--output_dir", "-o", type=Path, help="Output files directory")
    parser.add_argument("--type", "-t", type=str, choices=["video"], default="video", help="Input files type")
    parser.add_argument("--device", "-d", type=str, choices=["cuda", "cpu"], help="device type")
    parser.add_argument("--log_dir", "-l", type=Path, default="logs", help="Log directory")
    args = parser.parse_args()

    current_date = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")

    input_dir = args.input_dir
    output_dir = args.output_dir
    block_type = args.type
    device = args.device
    log_dir = args.log_dir

    logger_file = log_dir / f"to_mm_{current_date}.log"
    logger.add(logger_file, encoding="utf-8", rotation="500MB")

    if block_type == "video":
        if not input_dir.exists() or not input_dir.is_dir():
            raise ValueError(f"输入目录无效: {input_dir}")

        # 👇 获取视频列表
        videos: List[Path] = sorted(input_dir.glob("*.mp4"))

        if not videos:
            logger.warning(f"输入目录中未找到视频文件: {input_dir}")
            return

        process_video_to_parquets(
            videos=videos,
            output_dir=output_dir,
            use_auth_token=os.getenv("WHISPERX_API_KEY"),
            device=device
        )
    else:
        logger.warning(f"尚未处理 block_type: {block_type}")


if __name__ == "__main__":
    main()
