# Spark streaming near realtime

## Installation
Download [this file](https://dlcdn.apache.org/spark/spark-3.5.3/spark-3.5.3-bin-hadoop3.tgz), extract it using `tar xvzf <file name>.tgz`

Edit `~/.profile`, add the following lines
```
export SPARK_HOME=<path to spark directory>
export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
export PYSPARK_PYTHON=<path to python, e.g. /usr/bin/python3>
```

Spark submit spark streaming

```
spark-submit --packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 streaming.py
```
