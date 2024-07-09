from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql.types import StringType, StructField, StructType, IntegerType
from pyspark.sql.functions import from_json, current_timestamp, current_date, udf, col, dense_rank, collect_list, struct
from pyspark.sql.functions import sum as _sum
from datetime import date, timedelta
from pyspark.errors.exceptions.captured import AnalysisException
from py4j.protocol import Py4JJavaError
from datetime import date, timedelta, datetime

KAFKA_HOST = "localhost"
KAFKA_BROKER1_PORT = 9091
KAFKA_BROKER2_PORT = 9092
KAFKA_BROKER3_PORT = 9093

json_schema = StructType([
    StructField('domain', StringType(), True),
    StructField('label', StringType(), True),
    StructField('subscriberid', StringType(), True),
    StructField('count', IntegerType(), True)
])

def table_name_today():
    today = date.today()
    year = f"y{today.year}"
    month = f"m{str(today.month).zfill(2)}"
    day = f"d{str(today.day).zfill(2)}"

    return f"url_{year}_{month}_{day}"

def table_name_previous_day():
    previous_day = date.today() - timedelta(days=1)
    year = f"y{previous_day.year}"
    month = f"m{str(previous_day.month).zfill(2)}"
    day = f"d{str(previous_day.day).zfill(2)}"

    return f"global_temp.url_{year}_{month}_{day}"

def foreach_batch_function_hdfs(df, epoch_id):
    df.write.mode('append').parquet(f"file:////home/tuanvm/spark_streaming/data/{str(date.today())}.parquet")

def foreach_batch_function_redis(df, epoch_id):
    now = datetime.now()
    if datetime.now().hour == 0 and 0 <= now.minute and now.minute <= 2:
        if spark.catalog.tableExists(f"global_temp.{table_name_previous_day()}"):
            spark.catalog.dropGlobalTempView(table_name_previous_day())

    try:
        temp_df = spark.sql(f"SELECT * from global_temp.{table_name_today()}")
    except AnalysisException:
        temp_df = spark.createDataFrame(spark.sparkContext.emptyRDD(), columns)
    finally:
        temp_df.show(truncate=False)
    
    try:
        df_sum_up_previous_6_days = spark.read\
                            .format("org.apache.spark.sql.redis")\
                            .option("table", "topic-sum-up").load()
    except Py4JJavaError:
        df_sum_up_previous_6_days = spark.createDataFrame(spark.sparkContext.emptyRDD(), columns)

    current_union_temp_df = df.union(temp_df)
    current_union_temp_df = current_union_temp_df.union(df_sum_up_previous_6_days)

    current_union_temp_df = current_union_temp_df.groupBy("label", "subscriberid").agg(_sum("count").alias("count"))
    current_union_temp_df.createOrReplaceGlobalTempView(table_name_today())

    ranked_df = current_union_temp_df.withColumn("rank", dense_rank().over(window_spec))
    top5_df = ranked_df.filter(col("rank") <= 5)

    grouped_df = top5_df.withColumn("timestamp", current_timestamp())\
                        .withWatermark("timestamp", "1 minutes")\
                        .groupBy("subscriberid")\
                        .agg(collect_list(struct(col("label"), col("count"))).alias("label_count"))

    concatenate_label_count_udf = udf(concatenate_label_count, StringType())
    result_df = grouped_df.withColumn("label_count", concatenate_label_count_udf(col("label_count"))).select("subscriberid", "label_count")
    
    #result_df.show(truncate=False)
    result_df.selectExpr("CAST(subscriberid AS STRING) AS key", "to_json(struct(*)) AS value").write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", f"{KAFKA_HOST}:{KAFKA_BROKER1_PORT},{KAFKA_HOST}:{KAFKA_BROKER2_PORT},{KAFKA_HOST}:{KAFKA_BROKER3_PORT}") \
            .option("topic", "url_processed") \
            .save()

    #result_df.write.format("org.apache.spark.sql.redis").option("table", f"{str(date.today()).replace('-', '_')}-hot-topic").option("key.column", "subscriberid").mode("overwrite").save()

def concatenate_label_count(pairs):
    return ",".join([f"{pair['label']}:{pair['count']}" for pair in pairs])

columns = StructType([StructField('label',
                                  StringType(), True),
                    StructField('subscriberid',
                                StringType(), True),
                    StructField('count',
                                IntegerType(), True)])

if __name__ == '__main__':

    spark: SparkSession = SparkSession.builder \
        .appName("Streaming from Kafka") \
        .config("spark.streaming.stopGracefullyOnShutdown", True) \
        .config("spark.executor.cores", "10") \
        .config("spark.executor.memory", "100G") \
        .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1') \
        .config("spark.sql.shuffle.partitions", 4) \
        .config("spark.redis.host", "localhost") \
        .config("spark.redis.port", "6379") \
        .master("spark://server195:7077") \
        .getOrCreate()

    window_spec = Window.partitionBy("subscriberid").orderBy(col("count").desc())
    # failOnDataLoss: https://stackoverflow.com/questions/64922560/pyspark-and-kafka-set-are-gone-some-data-may-have-been-missed
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", f"{KAFKA_HOST}:{KAFKA_BROKER1_PORT},{KAFKA_HOST}:{KAFKA_BROKER2_PORT},{KAFKA_HOST}:{KAFKA_BROKER3_PORT}") \
        .option("group.id", "tuan-1") \
        .option("enable.auto.commit", True) \
        .option("failOnDataLoss", "false") \
        .option("subscribe", "url") \
        .load()
    json_df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING) as msg_value")

    json_expanded_df = json_df.withColumn("msg_value", from_json(json_df["msg_value"], json_schema)).select("msg_value.*")

    exploded_df = json_expanded_df.select("label", "subscriberid", "count")

    write_hdfs_df = exploded_df.writeStream\
                                .format("parquet")\
                                .outputMode("append")\
                                .option("checkpointLocation", "file:////home/tuanvm/spark_streaming/data/checkpoint_dir_hdfs")\
                                .foreachBatch(foreach_batch_function_hdfs)\
                                .start()
    
    write_redis_df = exploded_df \
        .writeStream \
        .outputMode("append")\
        .format("console")\
        .option("checkpointLocation", "file:////home/tuanvm/spark_streaming/data/checkpoint_dir_redis")\
        .foreachBatch(foreach_batch_function_redis) \
        .start()
    
    # writing_df = exploded_df \
    #     .writeStream \
    #     .format("console") \
    #     .option("checkpointLocation","file:////home/tuanvm/spark_streaming/data/checkpoint_dir") \
    #     .outputMode("append") \
    #     .start()

    # todo: modify
    spark.streams.awaitAnyTermination()
