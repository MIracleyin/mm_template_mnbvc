"""
2025.03.19 miracleyin@live.com
"""
import base64
import hashlib
import json
from typing import Dict, Any, Optional, Union
from dataclasses import dataclass, field
from datetime import datetime

import pyarrow as pa

#: 时间 列的统一格式。各模态必须用同一个，否则同一列会出现 "20250319" 和
#: "2025-03-19 10:00:00.123456" 两种值，没法按时间筛。
TIMESTAMP_FORMAT = "%Y%m%d"

#: 块类型 的取值。集中在这里，避免各模态各写各的字面量。
#:
#: 历史上这一列出现过三套词汇：已发布的标准示例
#: (hf.co/datasets/miracleyin/example_mmdata_mnbvc) 用 文字/图片/音频，
#: chinaxiv 用 pdf/image-text-pair，video_block 用 视频。同一列中英文混着来，
#: 按块类型筛选就得写 `块类型 IN ('video', '视频')` 这种东西。统一到英文。
BLOCK_TYPE_TEXT = "text"
BLOCK_TYPE_IMAGE = "image"
BLOCK_TYPE_AUDIO = "audio"
BLOCK_TYPE_VIDEO = "video"
BLOCK_TYPE_PDF = "pdf"
BLOCK_TYPE_IMAGE_TEXT_PAIR = "image-text-pair"

#: 媒体列（图片/视频/音频）的 Arrow 类型。
#:
#: HuggingFace datasets 的 Image / Audio / Video 三个 feature 用的都是
#: struct<bytes, path>，媒体列按它定型，`cast_column("图片", Image())` 才能
#: 直接解码。base64 字符串和裸 binary 都会抛
#: ArrowNotImplementedError: Unsupported cast ... to struct。
#:
#: 想改回裸二进制的话，把这里换成 pa.large_binary() 即可——实测 15 页
#: 1191x1684 PNG，zstd 之后两者都是 9.662 MiB，path 那个小字符串压没了，
#: 所以 struct 不多花空间。代价只是读端要自己
#: PIL.Image.open(io.BytesIO(...))，用不了 HF 的 Image feature。
#: 无论选哪个，读端都可以用 decode_media() 屏蔽差异。
MEDIA_TYPE = pa.struct([("bytes", pa.large_binary()), ("path", pa.string())])

#: 显式声明的 Arrow schema。
#:
#: 不声明的后果是实打实的：原来用 pa.Table.from_pandas(df) 让 pandas 推断类型，
#: 某一批里全是 None 的列会被推成 pa.null()。跑一次示例数据出来的 schema 是
#: 页ID: null / 视频: null / 音频: null / OCR文本: null / STT文本: null——
#: 一旦另一批里这些列有值，两个 shard 就拼不起来。
BLOCK_SCHEMA = pa.schema(
    [
        ("实体ID", pa.string()),
        ("md5", pa.string()),
        ("块ID", pa.int32()),
        ("块类型", pa.string()),
        ("扩展字段", pa.string()),
        ("时间", pa.string()),
        ("页ID", pa.int32()),
        ("文本", pa.large_string()),
        ("图片", MEDIA_TYPE),
        ("视频", MEDIA_TYPE),
        ("音频", MEDIA_TYPE),
        ("OCR文本", pa.large_string()),
        ("STT文本", pa.large_string()),
    ]
)

#: 二进制字段，序列化时需要特殊处理
BINARY_FIELDS = ("图片", "视频", "音频")


def get_timestamp():
    return datetime.now().strftime(TIMESTAMP_FORMAT)


def get_md5(text: str) -> str:
    return hashlib.md5(text.encode()).hexdigest()


def get_content_md5(*parts: Union[bytes, str, None]) -> str:
    """对内容本身求 md5。

    原来是 get_md5(文件名)，哈希的是文件名而不是内容，既不能去重也不能校验
    完整性——两份内容相同、文件名不同的数据会得到两个不同的 md5，反过来
    文件被截断了 md5 也不会变。
    """
    digest = hashlib.md5()
    for part in parts:
        if part is None:
            continue
        digest.update(part if isinstance(part, bytes) else part.encode("utf-8"))
    return digest.hexdigest()


@dataclass
class mmDataBlock:
    """Base class for data blocks with customizable field mapping"""
    # 必填字段
    实体ID: str
    md5: str
    块ID: int
    块类型: str
    扩展字段: str
    时间: str = field(default_factory=get_timestamp)

    # 可空字段
    页ID: Optional[int] = None
    文本: Optional[str] = None
    图片: Optional[bytes] = None
    视频: Optional[bytes] = None
    音频: Optional[bytes] = None
    OCR文本: Optional[str] = None
    STT文本: Optional[str] = None

    def __post_init__(self):
        """验证必填字段并计算md5"""
        if not self.实体ID:
            raise ValueError("实体ID不能为空")

        if not self.块类型:
            raise ValueError("块类型不能为空")

        # 扩展字段统一成 JSON 字符串。原来 pdf 路径传 dict、image-text-pair
        # 路径传 json.dumps 的字符串、video 路径传 str(dict)（单引号，不是
        # 合法 JSON），同一列三种形态。
        self.扩展字段 = self._normalize_扩展字段(self.扩展字段)

        # 如果没有提供md5，则按内容自动计算
        if not self.md5:
            self.md5 = self.content_md5()

        # 验证块ID
        if not isinstance(self.块ID, int):
            raise TypeError("块ID必须是整数类型")

        if self.页ID is not None and not isinstance(self.页ID, int):
            raise TypeError("页ID必须是整数类型")

    @staticmethod
    def _normalize_扩展字段(value: Any) -> str:
        if value is None:
            return "{}"
        if isinstance(value, str):
            return value
        return json.dumps(value, ensure_ascii=False)

    def content_md5(self) -> str:
        """基于负载内容而非文件名的 md5"""
        return get_content_md5(self.图片, self.视频, self.音频, self.文本)

    def from_dict(self, dict_data: Dict[str, Any]) -> 'mmDataBlock':
        """从字典创建或更新实例"""
        # 处理二进制数据的反序列化
        for field_name in BINARY_FIELDS:
            if field_name in dict_data and isinstance(dict_data[field_name], str):
                try:
                    dict_data[field_name] = base64.b64decode(
                        dict_data[field_name])
                except Exception as e:
                    # 原来这里写的是 {field}，引用到的是 dataclasses.field
                    # 这个函数对象，报错信息里会打出 <function field at 0x...>
                    raise ValueError(f"无法解码{field_name}的二进制数据: {e}")

        for field_name, value in dict_data.items():
            if hasattr(self, field_name):
                setattr(self, field_name, value)

        self.扩展字段 = self._normalize_扩展字段(self.扩展字段)

        # 如果实体ID被更新，且未提供新的md5值，则重新计算md5
        if '实体ID' in dict_data and 'md5' not in dict_data:
            self.md5 = self.content_md5()

        return self

    def to_dict(self) -> Dict[str, Any]:
        """转换为 JSON 可序列化的字典，二进制转 Base64

        给 to_json() 用。**不要**拿它去建 Parquet：base64 会让二进制列变成
        字符串列，HuggingFace 就没法 cast_column("图片", Image()) 了。
        写 Parquet 请用 to_arrow_dict()。
        """
        result = {}
        for field_name, value in self.__dict__.items():
            if value is None:
                result[field_name] = None
            elif isinstance(value, bytes):
                # 二进制数据转Base64字符串
                result[field_name] = base64.b64encode(value).decode('utf-8')
            else:
                result[field_name] = value
        return result

    def to_arrow_dict(self) -> Dict[str, Any]:
        """转换为符合 BLOCK_SCHEMA 的字典，二进制原样保留"""
        result: Dict[str, Any] = {}
        for field_name in BLOCK_SCHEMA.names:
            value = getattr(self, field_name, None)
            if field_name in BINARY_FIELDS:
                result[field_name] = self._media_value(value)
            else:
                result[field_name] = value
        return result

    def _media_value(self, value: Optional[bytes]) -> Any:
        """按 MEDIA_TYPE 打包媒体字节"""
        if value is None:
            return None
        if pa.types.is_struct(MEDIA_TYPE):
            return {"bytes": value, "path": self.实体ID}
        return value

    def to_json(self) -> str:
        """转换为JSON字符串"""
        return json.dumps(self.to_dict(), ensure_ascii=False)

    @classmethod
    def from_json(cls, json_str: str) -> 'mmDataBlock':
        """从JSON字符串创建实例"""
        try:
            data_dict = json.loads(json_str)
            for field_name in BINARY_FIELDS:
                if isinstance(data_dict.get(field_name), str):
                    data_dict[field_name] = base64.b64decode(
                        data_dict[field_name])
            instance = cls(**{k: v for k, v in data_dict.items()
                              if k in cls.__annotations__})
            return instance
        except Exception as e:
            raise ValueError(f"从JSON创建实例失败: {e}")


def decode_media(value: Any) -> Optional[bytes]:
    """把媒体列的值还原成 bytes，兼容历史上出现过的三种形态。

    - ``{"bytes": ..., "path": ...}``：当前 MEDIA_TYPE
    - ``bytes``：MEDIA_TYPE 换成 pa.large_binary() 时
    - ``str``：2025 年那批 parquet，to_dict() 做了 base64

    读旧数据和读新数据用同一个函数，迁移期不用分支。
    """
    if value is None:
        return None
    if isinstance(value, dict):
        return value.get("bytes")
    if isinstance(value, bytes):
        return value
    if isinstance(value, str):
        return base64.b64decode(value)
    raise TypeError(f"无法识别的媒体列取值: {type(value).__name__}")


def blocks_to_table(blocks) -> pa.Table:
    """按 BLOCK_SCHEMA 建表。所有写 Parquet 的路径都应该走这里。"""
    return pa.Table.from_pylist(
        [block.to_arrow_dict() for block in blocks], schema=BLOCK_SCHEMA
    )
