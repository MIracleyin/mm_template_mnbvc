from dataclasses import asdict, dataclass
from pathlib import Path
from typing import List

import pandas as pd
import whisperx
from loguru import logger
from moviepy import VideoFileClip, AudioFileClip

from src.mm_data.core.models.mmdata_block import mmDataBlock, get_md5


@dataclass
class VideoBlock(mmDataBlock):
    """ 视频数据块 """

    def __repr__(self):
        return f"VideoBlock(实体ID={self.实体ID}, 块ID={self.块ID}, 块类型={self.块类型}, 时间={self.时间}, 扩展字段={self.扩展字段})"


class VideoProcessor:
    """视频处理类，负责模型加载和视频块生成"""

    def __init__(self, use_auth_token: str, device: str = "cuda", compute_type: str = "float16"):
        """初始化模型"""
        logger.info("Loading models...")
        self.model = whisperx.load_model("large-v3", device, compute_type=compute_type)
        logger.info(f"模型加载成功: {type(self.model)}")
        self.diarize_model = whisperx.DiarizationPipeline(use_auth_token=use_auth_token, device=device)
        logger.info(f"说话人识别模型加载成功: {type(self.diarize_model)}")
        self.device = device
        self.use_auth_token = use_auth_token

    def speech_to_text(self, audio_file: Path) -> dict:
        """从音频文件中提取文本"""
        logger.debug(f"Using existing model to transcribe audio file: {audio_file}")
        audio = whisperx.load_audio(str(audio_file))
        stt_result = self.model.transcribe(audio)
        diarize_segments = self.diarize_model(audio)
        result = whisperx.assign_word_speakers(diarize_segments, stt_result)
        return result

    def extract_audio(self, video_file: Path, audio_file: Path) -> None:
        """从视频文件中提取出音频文件"""
        logger.debug(f"Extracting audio from {video_file} to {audio_file}")
        video_file_clip = VideoFileClip(str(video_file))
        video_file_clip.audio.write_audiofile(str(audio_file), logger=None)
        video_file_clip.close()  # 释放资源

    def generate_block(self, video_file: Path, block_id: int) -> VideoBlock:
        """将视频文件转换为block"""
        logger.info(f"开始生成视频块: {video_file.name}, 块ID: {block_id}")

        if not video_file.exists():
            raise FileNotFoundError(f"视频文件不存在: {video_file}")

        # 生成临时音频文件路径
        audio_file = video_file.with_suffix(".mp3")

        # 提取音频
        if not audio_file.exists():
            self.extract_audio(video_file, audio_file)

        # 读取音频时长
        try:
            audio_clip = AudioFileClip(str(audio_file))
            duration = audio_clip.duration
            audio_clip.close()  # 释放音频资源
        except Exception as e:
            logger.error(f"读取音频时长失败: {e}")
            duration = None

        # stt信息提取
        stt = self.speech_to_text(audio_file)
        texts = [segment.get('text', '') for segment in stt['segments'] if isinstance(segment, dict)]
        full_text = ' '.join(texts)
        language = stt['language']

        with open(video_file, 'rb') as f:
            binary_data = f.read()

        # 创建扩展字段字典
        extends = {"duration": duration, "language": language}

        block = VideoBlock(
            实体ID=video_file.name,
            md5=get_md5(video_file.name),
            块ID=block_id,
            块类型="视频",
            时间=str(pd.Timestamp.now()),
            视频=binary_data,
            文本=full_text,
            STT文本=str(stt),
            扩展字段=extends
        )

        return block


def block_to_parquet(block: VideoBlock, parquet_file: Path) -> None:
    """将块实例存储为parquet格式"""
    logger.debug(f"将块存储为parquet文件: {parquet_file}")

    # 将 dataclass 转换为字典
    block_dict = asdict(block)

    # 将字典包装为单行 DataFrame
    df = pd.DataFrame([block_dict])

    # 写入 Parquet 文件
    df.to_parquet(parquet_file, index=False)


def process_video_to_parquets(videos: List[Path], output_dir: Path, use_auth_token: str, device: str) -> None:
    """批量将视频列表处理成 parquet 文件"""
    logger.info(f"开始批量处理视频文件，共 {len(videos)} 个视频，输出目录: {output_dir}")

    if not videos:
        logger.warning("未提供任何视频文件，程序中止")
        return

    for video_file in videos:
        if not video_file.exists() or not video_file.is_file():
            raise FileNotFoundError(f"视频文件不存在或无效: {video_file}")

    output_dir.mkdir(parents=True, exist_ok=True)

    # 初始化 VideoProcessor 实例
    processor = VideoProcessor(use_auth_token=use_auth_token, device=device)

    for idx, video_file in enumerate(videos, start=1):
        logger.info(f"处理文件: {video_file.name}，块ID: {idx}")

        try:
            block = processor.generate_block(video_file, block_id=idx)
            parquet_path = output_dir / f"{block.块ID}.parquet"
            block_to_parquet(block, parquet_path)
        except Exception as e:
            logger.error(f"处理文件 {video_file.name} 时出错: {e}", exc_info=True)

    logger.info("视频文件处理完成")
