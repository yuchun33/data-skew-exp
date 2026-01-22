# 使用官方 Python 輕量版
FROM python:3.9-slim

# 安裝必要的套件
RUN pip install --no-cache-dir pandas numpy pyarrow s3fs

# 將你的腳本複製到容器內
COPY data-skew-pandas-read.py /app/data-skew-pandas-read.py

WORKDIR /app

# 執行腳本
CMD ["python", "data-skew-pandas-read.py"]