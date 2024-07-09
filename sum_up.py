from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum
from datetime import date, timedelta

spark: SparkSession = SparkSession.builder \
        .appName("Insert postgres") \
        .config('spark.jars', 'file:////home/tuanvm/spark_streaming/postgresql-42.7.2.jar') \
        .config("spark.sql.shuffle.partitions", 4) \
        .config("spark.redis.host", "localhost") \
        .config("spark.redis.port", "6379") \
        .master("spark://mhtuan-HP:7077") \
        .getOrCreate()

df_read = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/postgres") \
    .option("dbtable", "public.url") \
    .option("user", "postgres") \
    .option("password", "postgres") \
    .option("driver", "org.postgresql.Driver") \
    .load()

df_read = df_read.select("label", "subscriberid", "count")
df_read = df_read.groupBy("label", "subscriberid").agg(_sum("count").alias("count"))

df_read.write\
        .format("org.apache.spark.sql.redis")\
        .option("table", 'topic-sum-up')\
        .option("key.column", "subscriberid")\
        .mode("overwrite")\
        .save()