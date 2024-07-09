from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, date_sub
from pyspark.sql.functions import sum as _sum

spark: SparkSession = SparkSession.builder \
        .appName("Insert postgres") \
        .config('spark.jars', 'file:////home/tuanvm/spark_streaming/postgresql-42.7.2.jar') \
        .config("spark.sql.shuffle.partitions", 4) \
        .master("spark://mhtuan-HP:7077") \
        .getOrCreate()

# df = spark.read.parquet(f"file:////home/tuanvm/spark_streaming/data/{str(date.today() - timedelta(days=1))}.parquet")
df = spark.read.format("csv").option("header", "true").load(f"file:////home/tuanvm/spark_streaming/test.csv")

df.show()

df = df.select("label", "subscriberid", "count")
df = df.groupBy("label", "subscriberid").agg(_sum("count").alias("count"))
df = df.withColumn("insert_date", date_sub(current_date(), 1))

df.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/postgres") \
    .option("dbtable", "public.url") \
    .option("user", "postgres") \
    .option("password", "postgres") \
    .option("driver", "org.postgresql.Driver") \
    .mode("append")\
    .save()

# df_read = spark.read \
#     .format("jdbc") \
#     .option("url", "jdbc:postgresql://localhost:5432/postgres") \
#     .option("dbtable", "public.url") \
#     .option("user", "postgres") \
#     .option("password", "postgres") \
#     .option("driver", "org.postgresql.Driver") \
#     .load()

# df_read.show()