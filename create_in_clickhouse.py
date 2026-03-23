from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import logging

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

def check_minio_connection():
    """Проверка подключения к MinIO"""
    try:
        hook = S3Hook('minio_conn')
        # check_for_bucket - рабочий метод для проверки существования бакета
        buckets = ['tickers']
        for bucket in buckets:
            exists = hook.check_for_bucket(bucket_name=bucket)
            logging.info(f"Bucket '{bucket}': {exists}")
        
        # Альтернатива - list_keys для просмотра файлов в бакете
        for bucket in buckets:
            if hook.check_for_bucket(bucket_name=bucket):
                keys = hook.list_keys(bucket_name=bucket)
                logging.info(f"Bucket '{bucket}' has {len(keys) if keys else 0} files")
                if keys:
                    for key in keys[:5]:  # первые 5 файлов
                        logging.info(f"  - {key}")
        
        return True
    except Exception as e:
        logging.error(f"Failed to connect to MinIO: {e}")
        raise

with DAG(
    'minio_check_dag',
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=['minio', 'check']
) as dag:
    
    check_connection = PythonOperator(
        task_id='check_minio_connection',
        python_callable=check_minio_connection,
    )
    