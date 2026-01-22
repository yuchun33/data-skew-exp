from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, lit, concat, floor, rand, explode
import pyspark.sql.functions as F

spark = (
    SparkSession.builder.appName("data-skew-manual-salting-gen")
    # .config("spark.sql.adaptive.enabled", "false")
    .config("spark.sql.autoBroadcastJoinThreshold", "-1").getOrCreate()
)

df_large = (
    spark.range(0, 500000000)
    .withColumn("join_key", when(col("id") < 480000000, 1).otherwise(col("id") % 10000))
    .withColumn("payload", lit("x" * 100))
)  # 填充資料大小

df_small = spark.range(0, 10000).withColumnRenamed("id", "join_key")


# 1. 對大表進行加鹽 (Salting)
salt_number = 100  # 打散成 20-100 份

df_large_salted = df_large.withColumn(
    "salted_key",
    when(
        col("join_key") == 1,
        concat(col("join_key"), lit("_"), floor(rand() * salt_number)),
    ).otherwise(concat(col("join_key"), lit("_"), lit("0"))),
)

# 2. 對小表進行膨脹 (Exploding)
# 小表必須包含所有可能的鹽值，才能保證 Join 得到資料
df_small_salted = (
    df_small.withColumn(
        "salt_array",
        when(
            col("join_key") == 1, F.array([lit(i) for i in range(salt_number)])
        ).otherwise(F.array([lit(0)])),
    )
    .withColumn("salt", explode(col("salt_array")))
    .withColumn("salted_key", concat(col("join_key"), lit("_"), col("salt")))
)

result = df_large_salted.join(df_small_salted, "salted_key").count()