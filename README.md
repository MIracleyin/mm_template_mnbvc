# MM Data Converter for MNBVC

## 简介

将文本文件转换为 MNBVC 多模态数据格式，并保存为 Parquet 文件。

## 使用

```bash
python main.py --input_file input.txt --output_file output.parquet --split_size 200 --log_dir logs
```

## 参数

- `input_file`: 输入文件路径
- `output_file`: 输出文件路径
- `split_size`: 每个 parquet 文件的大小
- `log_dir`: 日志文件路径

