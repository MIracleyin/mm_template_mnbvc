from dataclasses import asdict, dataclass
from pathlib import Path

import pandas as pd
import whisperx
from loguru import logger
from moviepy import *

from src.mm_data.core.models.mmdata_block import mmDataBlock, get_md5


@dataclass
class VideoBlock(mmDataBlock):
    """ Chinaxiv 数据块 """
    def __repr__(self):
        return f"VideoBlock(实体ID={self.实体ID}, 块ID={self.块ID}, 块类型={self.块类型}, 时间={self.时间}, 扩展字段={self.扩展字段})"

def speech_to_text(audio_file: Path, use_auth_token: str, device = "cuda", compute_type = "float16") -> dict:
    """从音频文件中提取文本"""
    model = whisperx.load_model("large-v3", device, compute_type=compute_type)
    audio = whisperx.load_audio(str(audio_file))
    stt_result = model.transcribe(audio)
    diarize_model = whisperx.DiarizationPipeline(use_auth_token=use_auth_token, device=device)
    diarize_segments = diarize_model(audio)
    result = whisperx.assign_word_speakers(diarize_segments, stt_result)
    return result

def extract_audio(video_file: Path, audio_file: Path) -> None:
    """从视频文件中提取出音频文件"""
    video_file_clip = VideoFileClip(video_file)
    video_file_clip.audio.write_audiofile(audio_file)

def generate_block(video_file: Path, block_id: int, use_auth_token: str, device: str) -> VideoBlock:
    """将视频文件转换为block"""

    if not video_file.exists():
        raise FileNotFoundError(f"视频文件不存在: {video_file}")

    # 生成临时音频文件路径
    audio_file = video_file.with_suffix(".mp3")

    # 提取音频
    extract_audio(video_file, audio_file)

    # 转录音频 + 说话人识别
    segments = speech_to_text(audio_file, use_auth_token=use_auth_token, device=device)

    with open(video_file, 'rb') as f:
        binary_data = f.read()

    block = VideoBlock(
        实体ID=video_file.name,
        md5=get_md5(video_file.name),
        块ID=block_id,
        块类型="视频",
        扩展字段=str(segments),
        视频=binary_data
    )

    return block

def block_to_parquet(block: VideoBlock, parquet_file: Path) -> None:
    """将块实例存储为parquet格式"""

    # 将 dataclass 转换为字典
    block_dict = asdict(block)

    # 将字典包装为单行 DataFrame
    df = pd.DataFrame([block_dict])

    # 写入 Parquet 文件
    df.to_parquet(parquet_file, index=False)

def process_video_to_parquets(video_dir: Path, output_dir: Path, use_auth_token: str, device: str) -> None:
    """批量将文件夹下的视频处理成parquet"""
    if not video_dir.exists() or not video_dir.is_dir():
        raise ValueError(f"无效的视频目录: {video_dir}")

    output_dir.mkdir(parents=True, exist_ok=True)

    # 获取所有视频文件
    video_files = sorted(video_dir.glob("*.mp4"))

    if not video_files:
        logger.warning(f"目录中未找到视频文件: {video_dir}")
        return

    for idx, video_file in enumerate(video_files, start=1):
        logger.info(f"处理文件: {video_file.name}，块ID: {idx}")

        try:
            block = generate_block(video_file, block_id=idx, use_auth_token=use_auth_token, device=device)
            parquet_path = output_dir / f"{block.块ID}.parquet"
            block_to_parquet(block, parquet_path)
        except Exception as e:
            logger.error(f"处理文件 {video_file.name} 时出错: {e}")