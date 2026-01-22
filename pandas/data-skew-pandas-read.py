import pandas as pd
import numpy as np
import time

minio_storage_options = {
    "key": "minioadmin",
    "secret": "minioadmin",
    "client_kwargs": {
        "endpoint_url": "http://minio-service:9000", # 注意 K8S 內部的 Service Name
        "verify": False
    },
    "config_kwargs": {"s3": {"addressing_style": "path"}}
}

print("--- 開始執行 Pandas (K8S/MinIO 模式) ---")
start_time = time.time()

# 1. 改成從 S3 讀取 Parquet
s3_path = "s3://iot-data-lake/bronze/raw/iot_log"

print(f"正在從 S3 下載並解析資料...")
try:
    df_large = pd.read_parquet(
        s3_path, 
        engine='pyarrow',
        columns=["material"],
        storage_options=minio_storage_options
    )
    print(f"資料讀取完畢，總筆數：{len(df_large)}")
    print(f"目前 DataFrame 佔用記憶體：{df_large.memory_usage(deep=True).sum() / 1024**3:.2f} GB")

except Exception as e:
    print(f"讀取失敗：{e}")
    exit()

# 2. 生成小表 
df_small = pd.DataFrame({
    "material": [f"M_{i:04d}" for i in range(10000)],
    "attr": ["info"] * 10000
})

# 3. 執行 Join 操作
print("開始執行 Join (Pandas Merge)...")
try:
    # Pandas 的 merge 是單執行緒運算，面對傾斜資料 (M_0001 佔 80%) 會非常慢
    result = df_large.merge(df_small, on="material")
    print(f"Join 完成，結果筆數：{len(result)}")
except MemoryError:
    print("錯誤：記憶體不足 (OOM)！Pandas 無法處理此規模的資料。")

print(f"總執行時間：{time.time() - start_time:.2f} 秒")