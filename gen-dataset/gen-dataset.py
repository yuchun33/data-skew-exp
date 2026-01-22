from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
import os

spark = (
    SparkSession.builder.appName("generate_skewed_iot_data")
    .getOrCreate()
)

TOTAL_ROWS = 250_000_000 # 10GB 約需要 2 億到 3 億筆資料 (視壓縮率而定)
SKEW_RATIO = 0.8  # 80% 的資料會集中在少數幾個 material 上
OUTPUT_PATH = "s3a://iot-data-lake/bronze/raw/iot_log/"

df = spark.range(0, TOTAL_ROWS)

# 2. 製造 Data Skew (資料傾斜)
df_skewed = df.withColumn(
    "material",
    F.when(F.col("id") < (TOTAL_ROWS * SKEW_RATIO), "M_0001").otherwise(
        F.concat(F.lit("M_"), F.format_string("%04d", (F.rand() * 10000).cast("int")))
    ),
)

# 3. 填充其他欄位
df_final = (
    df_skewed.withColumn("line", F.concat(F.lit("Line-"), (F.rand() * 10).cast("int")))
    .withColumn("machine", F.concat(F.lit("MC-"), (F.rand() * 50).cast("int")))
    .withColumn("module", F.concat(F.lit("MOD-"), (F.rand() * 5).cast("int")))
    .withColumn("slot", (F.rand() * 20).cast("int").cast("string"))
    .withColumn("product", F.concat(F.lit("PROD-"), (F.rand() * 100).cast("int")))
    .withColumn(
        "status",
        F.expr(
            "case when rand() > 0.99 then 'ERROR' when rand() > 0.01 then 'OK' else 'UNKNOWN' end"
        ),
    )
    .withColumn("count", (F.rand() * 100).cast("int"))
)

# 4. 寫入 MinIO (Parquet 格式)
df_final.drop("id").write.mode("overwrite").parquet(OUTPUT_PATH)

print(f"Successfully generated skewed data to {OUTPUT_PATH}")
