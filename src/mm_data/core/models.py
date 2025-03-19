"""
2025.03.19 miracleyin@live.com
"""
import base64
import hashlib
import json
from typing import Dict, Any, Optional
from dataclasses import dataclass, field
from datetime import datetime


def get_timestamp():
    return datetime.now().strftime("%Y%m%d")


def get_md5(text: str) -> str:
    return hashlib.md5(text.encode()).hexdigest()


@dataclass
class mmDataBlock:
    """Base class for data blocks with customizable field mapping"""
    # 必填字段
    实体ID: str
    md5: str
    块ID: int
    块类型: str
    时间: str = field(default_factory=get_timestamp)
    扩展字段: str

    # 可空字段
    页ID: Optional[str] = None
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

        # 如果没有提供md5，则自动计算
        if not self.md5:
            self.md5 = get_md5(self.实体ID)

        # 验证块ID
        if not isinstance(self.块ID, int):
            raise TypeError("块ID必须是整数类型")

    def from_dict(self, dict_data: Dict[str, Any]) -> 'mmDataBlock':
        """从字典创建或更新实例"""
        # 处理二进制数据的反序列化
        binary_fields = ['图片', '视频', '音频']
        for field_name in binary_fields:
            if field_name in dict_data and isinstance(dict_data[field_name], str):
                try:
                    dict_data[field_name] = base64.b64decode(
                        dict_data[field_name])
                except Exception as e:
                    raise ValueError(f"无法解码{field}的二进制数据: {e}")

        for field_name, value in dict_data.items():
            if hasattr(self, field_name):
                setattr(self, field_name, value)

        # 如果实体ID被更新，且未提供新的md5值，则重新计算md5
        if '实体ID' in dict_data and 'md5' not in dict_data:
            self.md5 = get_md5(self.实体ID)
            
        return self

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典，处理二进制数据的序列化"""
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

    def to_json(self) -> str:
        """转换为JSON字符串"""
        return json.dumps(self.to_dict(), ensure_ascii=False)

    @classmethod
    def from_json(cls, json_str: str) -> 'mmDataBlock':
        """从JSON字符串创建实例"""
        try:
            data_dict = json.loads(json_str)
            instance = cls(**{k: v for k, v in data_dict.items()
                              if k in cls.__annotations__})
            return instance
        except Exception as e:
            raise ValueError(f"从JSON创建实例失败: {e}")
        