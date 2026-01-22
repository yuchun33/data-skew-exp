from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, lit, concat, floor, rand, explode
import pyspark.sql.functions as F

spark = (
    SparkSession.builder.appName("data-skew-auto-broadcast-read")
    # .config("spark.sql.adaptive.enabled", "false")
    # .config("spark.sql.autoBroadcastJoinThreshold", "-1")
    .getOrCreate()
)

# 1. 改成從 MINIO 讀取
df_large = spark.read.parquet("s3a://iot-data-lake/bronze/raw/iot_log")
print(df_large.explain(True))

# 2. 建立物料清單小表
df_small = (
    spark.range(0, 10001)
    .withColumn("material", F.concat(F.lit("M_"), F.format_string("%04d", F.col("id"))))
    .withColumn("material_desc", F.concat(F.lit("Description of "), F.col("material")))
    .drop("id")
)

result_df = df_large.join(df_small, "material")

print(f"Total processed rows: {result_df.count()}")