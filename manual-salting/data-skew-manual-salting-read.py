from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, lit, concat, floor, rand, explode
import pyspark.sql.functions as F

spark = (
    SparkSession.builder.appName("data-skew-manual-salting-read")
    # .config("spark.sql.adaptive.enabled", "false")
    .config("spark.sql.autoBroadcastJoinThreshold", "-1").getOrCreate()
)

df_large = spark.read.parquet("s3a://iot-data-lake/bronze/raw/iot_log")

df_small = (
    spark.range(0, 10001)
    .withColumn("material", F.concat(F.lit("M_"), F.format_string("%04d", F.col("id"))))
    .withColumn("material_desc", F.concat(F.lit("Description of "), F.col("material")))
    .drop("id")
)

# 1. 對大表進行加鹽 (Salting)
salt_number = 400  # 8G 拆成 200MB

# --- 大表修正 ---
df_large_salted = df_large.withColumn(
    "salted_key",
    when(
        col("material") == "M_0001",
        concat(col("material"), lit("_"), (rand() * salt_number).cast("int"))
    ).otherwise(concat(col("material"), lit("_0"))) # 統一使用 _0
)

# --- 小表修正 ---
df_small_salted = (
    df_small.withColumn(
        "salt_array",
        when(
            col("material") == "M_0001", 
            F.array([lit(i) for i in range(salt_number)])
        ).otherwise(F.array([lit(0)]))
    )
    .withColumn("salt", explode(col("salt_array")))
    .withColumn("salted_key", concat(col("material"), lit("_"), col("salt")))
)

result_df = df_large_salted.join(df_small_salted, "salted_key")

print(f"Total processed rows: {result_df.count()}")