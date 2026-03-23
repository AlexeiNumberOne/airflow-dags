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
        buckets = ['prod', 'tickers']
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

def list_files_in_bucket(bucket_name, **context):
    """Список файлов в бакете"""
    hook = S3Hook('minio_conn')
    logging.info(f"Listing files in bucket: {bucket_name}")
    
    try:
        objects = hook.list_keys(bucket_name=bucket_name)
        if objects:
            logging.info(f"Found {len(objects)} files:")
            for obj in objects:
                logging.info(f"  - {obj}")
        else:
            logging.info(f"No files found in bucket '{bucket_name}'")
        
        context['ti'].xcom_push(key='file_list', value=objects)
        return objects
    except Exception as e:
        logging.error(f"Error listing bucket {bucket_name}: {e}")
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
    
    list_prod_files = PythonOperator(
        task_id='list_prod_files',
        python_callable=list_files_in_bucket,
        op_kwargs={'bucket_name': 'prod'},
    )