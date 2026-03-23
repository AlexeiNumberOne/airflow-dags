import clickhouse_connect

from airflow import DAG
from airflow.operators.python import PythonOperator

def create():
    client = clickhouse_connect.get_client(
        host='clickhouse',
        port=8123,
        username='clickhouse',
        password='clickhouse',
        database='clickhouse'
    )

    with open('/opt/airflow/dags/sql/clickhouse.sql', 'r') as file:
        sql = file.read()
    
    sql = sql.split(';')
    
    for query in sql:
        if query:
            client.command(query)
        else:
            continue

with DAG(
    dag_id = 'create_in_clickhouse',
    schedule_interval=None,
    catchup=False,
    template_searchpath=['/opt/airflow/dags/sql']
) as dag:

    create = PythonOperator(
        task_id="create",
        python_callable=create,
    )