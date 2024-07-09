from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import pendulum
import psycopg2
import redis
from datetime import date, timedelta

submit_insert_command = """
spark-submit --jars /home/tuanvm/spark_streaming/postgresql-42.7.2.jar /home/tuanvm/spark_streaming/insert_db.py
"""
submit_sum_up_command = """
spark-submit --jars /home/tuanvm/spark_streaming/spark-redis_2.12-3.0.0-jar-with-dependencies.jar,/home/tuanvm/spark_streaming/postgresql-42.7.2.jar /home/tuanvm/spark_streaming/sum_up.py
"""

def delete_expired_data_pg():
    conn = psycopg2.connect(
                host="localhost",
                port=5432,
                user="postgres",
                password="postgres",
                database="postgres")
    
    cur = conn.cursor()

    cur.execute("DELETE FROM public.url WHERE insert_date <= (now()::DATE - 7)")
    conn.commit()

    cur.close()
    conn.close()

def delete_expired_keys_redis():
    r = redis.Redis(host='localhost', port=6379, decode_responses=True)

    previous_day = str(date.today() - timedelta(days=1)).replace("-", "_")
    keys = r.keys(f"{previous_day}-hot-topic")
    
    r.delete(*keys)

dag = DAG(
    'spark_submit_insert_pg',
    description='Read parquet, insert postgres',
    schedule="0 0 * * *", # every 00:00 of new day
    start_date=pendulum.datetime(2024, 7, 3, tz='Asia/Ho_Chi_Minh'),
    catchup=False
)

t1 = PythonOperator(
    task_id='remove',
    python_callable=delete_expired_data_pg,
    provide_context=True,
)

t2 = BashOperator(
    task_id='insert',
    bash_command=submit_insert_command,
    dag=dag
)

t3 = BashOperator(
    task_id='sum_up',
    bash_command=submit_sum_up_command,
    dag=dag
)

t4 = PythonOperator(
    task_id='delete_keys_redis',
    python_callable=delete_expired_keys_redis,
    provide_context=True,
)

t1 >> t2 >> t3 >> t4