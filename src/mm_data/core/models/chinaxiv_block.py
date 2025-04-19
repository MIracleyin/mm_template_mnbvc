from src.mm_data.core.models.mmdata_block import mmDataBlock
from pathlib import Path
from typing import List
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as parquet
from loguru import logger
from src.mm_data.core.file_handlers import get_pdf_bytes, get_img_bytes_and_size
from src.mm_data.core.processor import get_md5
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
    
    blocks, block_id = [], 0
    
    # 读取 docling_output_dir 下的所有文件
    pdf_file = input_file
    
    pdf_data = get_pdf_bytes(pdf_file)
    pdf_name = pdf_file.name
    
    json_file = docling_output_dir / (input_file.stem + ".json")
    json_data = json.load(json_file.open("r", encoding="utf-8"))
    
    md_file = docling_output_dir / (input_file.stem + ".md")
    md_data = md_file.open("r", encoding="utf-8").read()
    
    blocks.append(ChinaxivBlock(
        实体ID=pdf_name,
        块ID=block_id,
        块类型="pdf",
        扩展字段=json_data,
        图片=pdf_data,
        文本=md_data
    ))
    
    logger.info(
        f"process {input_file} done, {len(blocks)} blocks generated")
    
    return blocks

def chinaxiv_to_image_text_pair_blocks(input_file: Path) -> List[ChinaxivBlock]:
    """将 Chinaxiv 文件转换为 ChinaxivImageTextPairBlock 列表"""
    docling_output_dir = input_file.parent / \
        f"{input_file.stem}_docling_output"
    
    blocks, block_id = [], 0

    pages_dir = docling_output_dir / "pages"
    img_files = sorted(list(pages_dir.glob("*.png")),
                       key=lambda x: int(x.stem.split("page-")[1]))  # 按页码排序
    md_files = sorted(list(pages_dir.glob("*.md")),
                      key=lambda x: int(x.stem.split("page-")[1]))  # 按页码排序
    assert len(img_files) == len(md_files), logger.error(
        f"The number of image files and md files is {len(img_files)}")
    
    for page_id, (img_file, md_file) in enumerate(zip(img_files, md_files)):
        img_data, img_size = get_img_bytes_and_size(img_file)
        md_data = md_file.open("r", encoding="utf-8").read()
        
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
            块类型="image-text-pair",
            扩展字段=json.dumps(json_data),
            图片=img_data,
            文本=md_data,
            md5=get_md5(img_file.name)
        ))
        
    logger.info(
        f"process {input_file} done, {len(blocks)} blocks generated")
    
    return blocks
    
def get_blocks(input_file: Path, block_type: str) -> List[ChinaxivBlock]:
    if block_type == "pdf":
        return chinaxiv_to_pdf_blocks(input_file)
    elif block_type == "image-text-pair":
        return chinaxiv_to_image_text_pair_blocks(input_file)
    else:
        raise ValueError(f"Invalid block type: {block_type}")
    
def batch_to_parquet(output_file: Path, split_size: int, batchs: List[List[ChinaxivBlock]]):
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
        output_file_last = output_file.parent / f"{output_file.stem}_{split_count}.parquet"
        table = pa.Table.from_pandas(df)
        parquet.write_table(table,
                            output_file_last,
                            compression='zstd',  # 可选: 'snappy', 'gzip', 'brotli', 'zstd', 'lz4'
                            row_group_size=100000  # 优化读写性能的行组大小
                            )
        logger.info(f"batch {split_count} done, {output_file_last} generated")