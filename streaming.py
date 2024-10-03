from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql.types import StringType, StructField, StructType, IntegerType
from pyspark.sql.functions import from_json, current_timestamp, current_date, udf, col, dense_rank, collect_list, struct, lit
from pyspark.sql.functions import sum as _sum, max as _max
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
    StructField('count', IntegerType(), True),
    StructField('created_at_sec', IntegerType(), True),
    StructField('created_at', StringType(), True)
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

def foreach_batch_function_kafka(df: DataFrame, epoch_id):
    """
    process df, write to redis
    """
    now = datetime.now()
    if datetime.now().hour == 0 and 0 <= now.minute and now.minute <= 2:
        if spark.catalog.tableExists(f"global_temp.{table_name_previous_day()}"):
            spark.catalog.dropGlobalTempView(table_name_previous_day())

    # select temp view
    try:
        temp_df = spark.sql(f"SELECT * from global_temp.{table_name_today()}")
    except AnalysisException:
        temp_df = spark.createDataFrame(spark.sparkContext.emptyRDD(), columns)
    finally:
        temp_df.show(truncate=False)
    
    df_sum_up_previous_6_days = spark.createDataFrame(spark.sparkContext.emptyRDD(), columns)

    # union 3 dataframes
    current_union_temp_df = df.union(temp_df)
    current_union_temp_df = current_union_temp_df.union(df_sum_up_previous_6_days)

    # group by then count the frequency of each topic for each subscriberid
    current_union_temp_df = current_union_temp_df.groupBy("label", "subscriberid").agg(_sum("count").alias("count"), _max("created_at").alias("created_at"))
    # create global temp view
    result_df = current_union_temp_df.withColumn("done_at", lit(str(datetime.now())))
    current_union_temp_df.createOrReplaceGlobalTempView(table_name_today())
    
    # write to kafka
    result_df.selectExpr("CAST(subscriberid AS STRING) AS key", "to_json(struct(*)) AS value").write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", f"{KAFKA_HOST}:{KAFKA_BROKER1_PORT}") \
            .option("topic", "spark-topic-count.access-count") \
            .save()

def concatenate_label_count(pairs):
    return ",".join([f"{pair['label']}:{pair['count']}" for pair in pairs])

columns = StructType([StructField('label',
                                  StringType(), True),
                    StructField('subscriberid',
                                StringType(), True),
                    StructField('count',
                                IntegerType(), True),
                    StructField('created_at',
                                StringType(), True)])

if __name__ == '__main__':

    spark: SparkSession = SparkSession.builder \
        .appName("Streaming from Kafka") \
        .config("spark.streaming.stopGracefullyOnShutdown", True) \
        .config("spark.executor.cores", "4") \
        .config("spark.executor.memory", "4G") \
        .config("spark.executor.instances", "5") \
        .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1') \
        .config("spark.sql.shuffle.partitions", 4) \
        .master("spark://mhtuan-HP:7077") \
        .getOrCreate()
    
    spark.conf.set("spark.executor.memory", "40g")

    window_spec = Window.partitionBy("subscriberid").orderBy(col("count").desc())
    # failOnDataLoss: https://stackoverflow.com/questions/64922560/pyspark-and-kafka-set-are-gone-some-data-may-have-been-missed
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", f"{KAFKA_HOST}:{KAFKA_BROKER1_PORT}") \
        .option("group.id", "tuan-1") \
        .option("enable.auto.commit", True) \
        .option("failOnDataLoss", "false") \
        .option("subscribe", "spark-topic-count.url") \
        .load()
    json_df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING) as msg_value")

    json_expanded_df = json_df.withColumn("msg_value", from_json(json_df["msg_value"], json_schema)).select("msg_value.*")

    exploded_df = json_expanded_df.select("label", "subscriberid", "count", "created_at")
    
    write_redis_df = exploded_df \
        .writeStream \
        .outputMode("append")\
        .format("console")\
        .option("checkpointLocation", "file:////home/mhtuan/work/ct1/streaming/spark_streaming/checkpoint_dir_redis")\
        .foreachBatch(foreach_batch_function_kafka) \
        .start()
    
    spark.streams.awaitAnyTermination()
