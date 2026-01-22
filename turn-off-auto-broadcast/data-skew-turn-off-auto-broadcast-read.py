from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, lit, concat, floor, rand, explode
import pyspark.sql.functions as F

spark = (
    SparkSession.builder.appName("data-skew-turn-off-auto-broadcast-read")
    # .config("spark.sql.adaptive.enabled", "false")
    .config("spark.sql.autoBroadcastJoinThreshold", "-1").getOrCreate()
)

# 1. 改成從 MINIO 讀取
df_large = spark.read.parquet("s3a://iot-data-lake/bronze/raw/iot_log")

# 2. 建立小表
df_small = (
    spark.range(0, 10001)
    .withColumn("material", F.concat(F.lit("M_"), F.format_string("%04d", F.col("id"))))
    .withColumn("material_desc", F.concat(F.lit("Description of "), F.col("material")))
    .drop("id")
)

# 3. 執行 Join
# 這裡會發生嚴重傾斜，因為 80% 的資料都會擠在處理 "M_0001" 的那個 Task 上
result_df = df_large.join(df_small, "material")

print(f"Total processed rows: {result_df.count()}")