import logging
import io
import time
import requests

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from requests.exceptions import Timeout, RequestException
from plugins import for_delay

default_args = {
    'owner':'airflow',
    'start_date': datetime(2026, 3, 19), 
    'retries': 3,
    "catchup": True,
    "retry_delay": timedelta(minutes=5),
}

def get_date(**context):
    start_date = context["data_interval_start"].strftime("%Y-%m-%d")
    return start_date


def extract_and_load_from_api_to_minio(**context):

    tickers = ['AAPL']

    start_date = get_date(**context)
    logging.info(f"Сбор данных с API за: {start_date}")
    
    s3_hook = S3Hook('minio_conn')
    bucket_name = 'tickers'
    if not s3_hook.check_for_bucket(bucket_name):
        s3_hook.create_bucket(bucket_name)
        logging.info(f"Бакет {bucket_name} создан")
    else:
        logging.info(f"Бакет {bucket_name} найден")

    API_KEY = Variable.get("API_KEY") # API KEY https://massive.com/
  
    for index, ticker in enumerate(tickers):
        for_delay.check_pause()

        logging.info(f"Tikcer :{ticker} (Запрос №{index+1})")

        for attemp in range(3):

            if attemp > 0:
                logging.warning(f"Попытка №{attemp+1}")

                for_delay.check_pause()

            try:
                url = f'https://api.massive.com/v2/aggs/ticker/{ticker}/range/1/minute/{start_date}/{start_date}'
                params = {
                    "adjusted": "true",
                    "sort": "asc",
                    "limit": 50000,
                    "apiKey": API_KEY
                }

                response = requests.get(url, params=params,timeout=(5,5))

                if response.status_code == 429:
                    logging.error("Превышен лимит запросов, паузна 60 секунд!")
                    time.sleep(60)
                    continue

                if response.status_code != 200:
                    logging.error(f"API вернул ошибку {response.status_code}: {response.text[:200]}")
                    raise

                result = response.text

                s3_hook.load_string(
                    string_data=result,
                    key=f"foreign/{ticker}/{start_date}/response_{start_date}.json",
                    bucket_name=bucket_name,
                    replace=True
                )
                
                break

            except Timeout:
                logging.error(f"Таймаут при запросе {ticker} (API не ответил за 5 секунд)")
                continue
            except RequestException as e:
                logging.error(f"Ошибка запроса для {ticker}: {e}")
                continue
            except Exception as e:
                logging.error(f"Неожиданная ошибка для {ticker}: {e}")
                continue
        
with DAG(
    dag_id='from_api_to_raw',
    schedule="0 5 * * 1-5",     
    default_args=default_args,
    tags=["docker"],
    max_active_tasks=1,
    max_active_runs=1,
) as dag:

    start = EmptyOperator(
        task_id="start",
    )

    extract_and_load_from_api_to_minio = PythonOperator(
        task_id="extract_and_load_from_api_to_minio",
        python_callable=extract_and_load_from_api_to_minio,
    )

    end = EmptyOperator(
        task_id="end",
    )

    start >> extract_and_load_from_api_to_minio >> end