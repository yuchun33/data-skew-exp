import pandas as pd
import numpy as np
import time

print("--- 開始執行 Pandas 模式 ---")
start_time = time.time()

# 1. 生成 1 億筆資料
# 注意：這一步在 Pandas 中會消耗大量記憶體
num_rows = 100000000
df_large = pd.DataFrame({"id": np.arange(num_rows)})

# 2. 模擬資料傾斜 (80% 的資料 key = 1)
df_large["join_key"] = np.where(df_large["id"] < 80000000, 1, df_large["id"] % 100000)

# 3. 填充資料大小 (100 字元的字串)
# 在 Pandas 中，這會讓 DataFrame 迅速膨脹到 10GB 以上
df_large["payload"] = "x" * 100

print(f"資料生成完畢，耗時：{time.time() - start_time:.2f} 秒")

# 4. 生成小表
df_small = pd.DataFrame({"join_key": np.arange(10000)})

# 5. 執行 Join 操作
# 極度危險區：1 億筆資料的 Join 在 Pandas 中是單執行緒，且非常吃記憶體
print("開始執行 Join...")
try:
    result = df_large.merge(df_small, on="join_key")
    print(f"Join 完成，結果筆數：{len(result)}")
except MemoryError:
    print("錯誤：記憶體不足 (OOM)！Pandas 無法處理此規模的資料。")

print(f"總執行時間：{time.time() - start_time:.2f} 秒")
