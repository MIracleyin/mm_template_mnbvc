from src.mm_data.core.models.mmdata_block import (
    BLOCK_TYPE_IMAGE_TEXT_PAIR,
    BLOCK_TYPE_PDF,
    mmDataBlock,
)
from pathlib import Path
from typing import List
from loguru import logger
from src.mm_data.core.file_handlers import get_img_bytes_and_size
from src.mm_data.core.processor import write_blocks
import json

"""
Chinaxiv 数据
1. pdf block
    1. 包含 PDF 文件的二进制数据和 PDF 文件的名称, 块类型为 "pdf"
    2. metadata 信息为 docling 的解析结果

2. image-text pair block
    1. 包含图片的二进制数据和文本对, 块类型为 "image-text-pair"
    2. metadata 信息为 {"page_id": 1, "page_image_size": {"width": 100, "height": 100}, "page_text_length": 100}
"""

class ChinaxivBlock(mmDataBlock):
    """ Chinaxiv 数据块 """
    def __repr__(self):
        return f"ChinaxivBlock(实体ID={self.实体ID}, 块ID={self.块ID}, 块类型={self.块类型}, 时间={self.时间}, 扩展字段={self.扩展字段})"

def chinaxiv_to_pdf_blocks(input_file: Path) -> List[ChinaxivBlock]:
    """将 Chinaxiv 文件转换为 ChinaxivPDFBlock 列表"""
    docling_output_dir = input_file.parent / \
        f"{input_file.stem}_docling_output"

    if not docling_output_dir.is_dir():
        raise FileNotFoundError(f"未找到 docling 输出目录: {docling_output_dir}")

    blocks, block_id = [], 0

    # 读取 docling_output_dir 下的所有文件
    pdf_file = input_file

    pdf_name = pdf_file.name

    json_file = docling_output_dir / (input_file.stem + ".json")
    json_data = json.load(json_file.open("r", encoding="utf-8"))

    md_file = docling_output_dir / (input_file.stem + ".md")
    md_data = md_file.open("r", encoding="utf-8").read()

    blocks.append(ChinaxivBlock(
        实体ID=pdf_name,
        md5="",  # 留空，由 mmDataBlock 按内容计算
        块ID=block_id,
        块类型=BLOCK_TYPE_PDF,
        # 原来是 扩展字段=json_data 直接传 dict，而 image-text-pair 那条路径传的
        # 是 json.dumps 后的字符串，同一列两种类型。现在统一交给 mmDataBlock
        # 序列化，两条路径出来的都是 JSON 字符串。
        扩展字段={"docling": json_data, "source_pdf": pdf_name},
        # 原来是 图片=pdf_data，把 PDF 字节塞进了「图片」列。PDF 不是图片，
        # 这个块的价值在 docling 解析出来的文本，原始 PDF 应该留在 L0 由
        # 实体ID 索引，而不是在这里占一个语义不对的二进制列。
        文本=md_data,
    ))

    logger.info(
        f"process {input_file} done, {len(blocks)} blocks generated")

    return blocks

def chinaxiv_to_image_text_pair_blocks(input_file: Path) -> List[ChinaxivBlock]:
    """将 Chinaxiv 文件转换为 ChinaxivImageTextPairBlock 列表"""
    docling_output_dir = input_file.parent / \
        f"{input_file.stem}_docling_output"

    blocks = []

    pages_dir = docling_output_dir / "pages"
    # 目录不存在时，下面的 glob 会返回空列表、assert 0 == 0 通过、循环体一次
    # 不执行，最后打一条 "0 blocks generated" 的 INFO 就当成功了。仓库自带的
    # data/chinaxivlist.txt 指的就是一个不存在的路径，跑出来正好 0 块。
    if not pages_dir.is_dir():
        raise FileNotFoundError(f"未找到分页目录: {pages_dir}")

    img_files = sorted(list(pages_dir.glob("*.png")),
                       key=lambda x: int(x.stem.split("page-")[1]))  # 按页码排序
    md_files = sorted(list(pages_dir.glob("*.md")),
                      key=lambda x: int(x.stem.split("page-")[1]))  # 按页码排序
    if len(img_files) != len(md_files):
        raise ValueError(
            f"{pages_dir}: 图片 {len(img_files)} 个，md {len(md_files)} 个，数量不一致")
    if not img_files:
        raise ValueError(f"{pages_dir} 下没有分页文件")

    # 原来 block_id 初始化成 0 之后从未自增，一个文档里所有块的 块ID 都是 0。
    for block_id, (img_file, md_file) in enumerate(zip(img_files, md_files)):
        img_data, img_size = get_img_bytes_and_size(img_file)
        md_data = md_file.open("r", encoding="utf-8").read()

        page_id = int(img_file.stem.split("page-")[1]) - 1  # docling 从 1 开始编号

        json_data = {
            "page_id": page_id,
            "page_image_size": {
                "width": img_size[0],
                "height": img_size[1],
            },
            "page_text_length": len(md_data),
        }

        blocks.append(ChinaxivBlock(
            实体ID=img_file.name,
            块ID=block_id,
            块类型=BLOCK_TYPE_IMAGE_TEXT_PAIR,
            扩展字段=json_data,
            # 原来 page_id 只写进扩展字段的 JSON 里，专门的 页ID 列一直是 None，
            # 而且因为整列全空，pandas 会把它推断成 pa.null() 类型。
            页ID=page_id,
            图片=img_data,
            文本=md_data,
            # 原来是 md5=get_md5(img_file.name)，哈希的是文件名。留空由
            # mmDataBlock 按内容计算。
            md5="",
        ))

    logger.info(
        f"process {input_file} done, {len(blocks)} blocks generated")

    return blocks

def get_blocks(input_file: Path, block_type: str) -> List[ChinaxivBlock]:
    if block_type == BLOCK_TYPE_PDF:
        return chinaxiv_to_pdf_blocks(input_file)
    elif block_type == BLOCK_TYPE_IMAGE_TEXT_PAIR:
        return chinaxiv_to_image_text_pair_blocks(input_file)
    else:
        raise ValueError(f"Invalid block type: {block_type}")

def batch_to_parquet(output_file: Path, split_size: int, batchs: List[List[ChinaxivBlock]]):
    """按 split_size 个 batch 一个文件写出。

    实现改为委托给 processor.write_blocks，两处的写法原本是重复的，而且都用
    pa.Table.from_pandas 让 pandas 推断类型。
    """
    batch_rows = []
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
        output_file_last = output_file.parent / f"{output_file.stem}_{split_count}.parquet"
        write_blocks(batch_rows, output_file_last)
        logger.info(f"batch {split_count} done, {output_file_last} generated")


__all__ = [
    "ChinaxivBlock",
    "chinaxiv_to_pdf_blocks",
    "chinaxiv_to_image_text_pair_blocks",
    "get_blocks",
    "batch_to_parquet",
]
