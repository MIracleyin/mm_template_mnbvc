
from pathlib import Path
from typing import Tuple
from PIL import Image as PILImage
from loguru import logger
import io

def get_img_bytes_and_size(img_path: Path) -> Tuple[bytes, Tuple[int, int]]:
    """将图片文件转换为二进制格式

    Args:
        img_path: 图片文件路径

    Returns:
        bytes: 图片文件的二进制数据
        Tuple[int, int]: 图片的宽度和高度
    """
    try:
        with open(img_path, 'rb') as file:
            image = PILImage.open(file)
            img_byte_arr = io.BytesIO()
            image.save(img_byte_arr, format=image.format)
            img_byte_arr = img_byte_arr.getvalue()
        return img_byte_arr, image.size
    except Exception as e:
        logger.error(f"图片转换二进制失败: {e}")
        return None

def get_pdf_bytes(pdf_path: Path) -> bytes:
    """将PDF文件转换为二进制格式

    Args:
        pdf_path: PDF文件路径

    Returns:
        bytes: PDF文件的二进制数据
    """
    try:
        # 打开PDF文件
        with open(pdf_path, 'rb') as file:
            # 创建二进制缓冲区
            pdf_byte_arr = io.BytesIO()
            # 将PDF内容写入缓冲区
            pdf_byte_arr.write(file.read())
            # 获取二进制数据
            pdf_binary = pdf_byte_arr.getvalue()

        return pdf_binary

    except Exception as e:
        logger.error(f"PDF转换二进制失败: {e}")
        return None
    
