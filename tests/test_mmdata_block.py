"""mmDataBlock / BLOCK_SCHEMA 的行为约束。

这些测试锁的是本次修掉的几个问题，避免以后再退回去。
"""
import base64
import json

import pyarrow as pa
import pytest

from src.mm_data.core.models.mmdata_block import (
    BLOCK_SCHEMA,
    BLOCK_TYPE_IMAGE_TEXT_PAIR,
    MEDIA_TYPE,
    blocks_to_table,
    decode_media,
    mmDataBlock,
)

PNG = b"\x89PNG\r\n\x1a\n" + b"fake" * 8


def make_block(**kw):
    base = dict(
        实体ID="doc-page-1.png",
        md5="",
        块ID=0,
        块类型=BLOCK_TYPE_IMAGE_TEXT_PAIR,
        扩展字段={"page_id": 0},
        页ID=0,
        文本="正文",
        图片=PNG,
    )
    base.update(kw)
    return mmDataBlock(**base)


# --- 扩展字段：三种输入形态都要归一成 JSON 字符串 -------------------------


@pytest.mark.parametrize("value,expected", [
    ({"a": 1}, {"a": 1}),
    ('{"a": 1}', {"a": 1}),
    (None, {}),
])
def test_扩展字段_always_json_string(value, expected):
    block = make_block(扩展字段=value)
    assert isinstance(block.扩展字段, str)
    assert json.loads(block.扩展字段) == expected


def test_扩展字段_keeps_chinese_readable():
    assert "中文" in make_block(扩展字段={"k": "中文"}).扩展字段


# --- md5：内容而不是文件名 -----------------------------------------------


def test_md5_is_over_content_not_the_name():
    a = make_block(实体ID="a.png")
    b = make_block(实体ID="完全不同的名字.png")
    assert a.md5 == b.md5, "同样的内容应该得到同样的 md5"

    c = make_block(图片=PNG + b"x")
    assert c.md5 != a.md5, "内容变了 md5 就该变"


def test_explicit_md5_is_respected():
    assert make_block(md5="deadbeef").md5 == "deadbeef"


# --- 页ID / 块ID 的类型 ---------------------------------------------------


def test_页ID_must_be_int():
    with pytest.raises(TypeError):
        make_block(页ID="0")


def test_块ID_must_be_int():
    with pytest.raises(TypeError):
        make_block(块ID="0")


# --- schema：全空的列也必须保住类型 ---------------------------------------


def test_all_null_columns_keep_their_declared_type():
    """原来用 pandas 推断，整列为空就成了 pa.null()，两个 shard 拼不起来。"""
    table = blocks_to_table([make_block()])
    assert table.schema == BLOCK_SCHEMA
    for name in ("视频", "音频", "OCR文本", "STT文本"):
        assert not pa.types.is_null(table.schema.field(name).type), name


def test_two_shards_with_different_null_columns_concatenate():
    t1 = blocks_to_table([make_block()])                      # 视频 全空
    t2 = blocks_to_table([make_block(视频=b"mp4", 图片=None)])  # 图片 全空
    assert pa.concat_tables([t1, t2]).num_rows == 2


def test_media_column_matches_the_declared_media_type():
    table = blocks_to_table([make_block()])
    assert table.schema.field("图片").type == MEDIA_TYPE


# --- 序列化 ---------------------------------------------------------------


def test_to_dict_base64s_binary_for_json_but_arrow_dict_does_not():
    block = make_block()
    assert base64.b64decode(block.to_dict()["图片"]) == PNG
    assert decode_media(block.to_arrow_dict()["图片"]) == PNG


def test_json_roundtrip_preserves_bytes():
    block = make_block()
    assert mmDataBlock.from_json(block.to_json()).图片 == PNG


def test_from_dict_error_message_names_the_field():
    """原来报错信息里插的是 dataclasses.field 函数对象。"""
    with pytest.raises(ValueError, match="图片"):
        make_block().from_dict({"图片": "不是合法的 base64!!"})


# --- decode_media 兼容三种历史形态 ----------------------------------------


@pytest.mark.parametrize("value", [
    {"bytes": PNG, "path": "x.png"},          # 当前
    PNG,                                       # 裸 binary
    base64.b64encode(PNG).decode(),            # 2025 年那批 parquet
])
def test_decode_media_handles_every_historical_shape(value):
    assert decode_media(value) == PNG


def test_decode_media_passes_none_through():
    assert decode_media(None) is None


def test_decode_media_rejects_nonsense():
    with pytest.raises(TypeError):
        decode_media(123)
