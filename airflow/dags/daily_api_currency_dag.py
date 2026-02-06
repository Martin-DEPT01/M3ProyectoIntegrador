from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
from airflow.datasets import Dataset


DS_COTIZACIONES = Dataset("mysql://rds/raw_cotizaciones")


with DAG(
    dag_id="dag1_cotizaciones_diario",
    description="Extrae cotizaciones desde la API, carga un json en S3 y luego carga en MySQL RDS capa raw",
    schedule_interval="@daily",  # Se ejecuta una vez al día (medianoche)
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['elt', 'diario', 'cotizaciones'],
) as dag:


    run_api_currency_raw = BashOperator(
        task_id="run_api_currency_raw",
        bash_command='python /opt/airflow/scripts/api_currency_raw.py'
    )

    run_api_currency_elt = BashOperator(
        task_id="run_api_currency_elt",
        bash_command='python /opt/airflow/scripts/api_currency_elt.py'
        outlets=[DS_COTIZACIONES] # <--- Avisa que actualizó cotizaciones
    )

    run_api_currency_raw >> run_api_currency_elt
