"""chinaxiv 转换路径的行为约束（跑仓库自带的示例数据）。"""
import json
from pathlib import Path

import pytest

from src.mm_data.core.models.chinaxiv_block import (
    chinaxiv_to_image_text_pair_blocks,
    get_blocks,
)
from src.mm_data.core.models.mmdata_block import BLOCK_SCHEMA, blocks_to_table

SAMPLE = Path("data/chinaxiv/202112.00050v1_单栏_文本_表格.pdf")
pytestmark = pytest.mark.skipif(
    not (SAMPLE.parent / f"{SAMPLE.stem}_docling_output" / "pages").is_dir(),
    reason="示例数据未通过 git-lfs 拉取",
)


@pytest.fixture(scope="module")
def blocks():
    return chinaxiv_to_image_text_pair_blocks(SAMPLE)


def test_block_id_increments(blocks):
    """原来 block_id 初始化成 0 之后从未自增，整个文档全是 0。"""
    assert [b.块ID for b in blocks] == list(range(len(blocks)))


def test_page_id_column_is_populated(blocks):
    """原来 page_id 只写进扩展字段，页ID 列一直是 None。"""
    assert [b.页ID for b in blocks] == list(range(len(blocks)))
    for b in blocks:
        assert json.loads(b.扩展字段)["page_id"] == b.页ID


def test_pages_are_in_reading_order(blocks):
    """按 page-2 / page-10 排序，字典序会把 10 排在 2 前面。"""
    nums = [int(b.实体ID.split("page-")[1].split(".")[0]) for b in blocks]
    assert nums == sorted(nums)


def test_every_block_has_both_modalities(blocks):
    for b in blocks:
        assert b.图片, b.实体ID
        assert b.文本 is not None, b.实体ID


def test_schema_is_stable(blocks):
    assert blocks_to_table(blocks).schema == BLOCK_SCHEMA


def test_missing_input_raises_instead_of_yielding_zero_blocks(tmp_path):
    """原来目录不存在时 glob 返回空、assert 0 == 0 通过、循环不执行，
    最后打一条 "0 blocks generated" 就当成功了。"""
    with pytest.raises(FileNotFoundError):
        chinaxiv_to_image_text_pair_blocks(tmp_path / "不存在.pdf")


def test_unknown_block_type_is_rejected():
    with pytest.raises(ValueError, match="Invalid block type"):
        get_blocks(SAMPLE, "视频")
