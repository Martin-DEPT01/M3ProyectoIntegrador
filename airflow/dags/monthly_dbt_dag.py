from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.datasets import Dataset
from datetime import datetime

# 1. Referenciamos los mismos nombres de Datasets que se definieron en el DAG 1 y 2
DS_COTIZACIONES = Dataset("mysql://rds/raw_cotizaciones")
DS_CSV_MENSUAL = Dataset("mysql://rds/csv_raw_mensual")



with DAG(
    dag_id="dag3_mensual_dbt",
    description="Ejecuta el modelo dbt completo junto con sus tests. dbt build",
    # EL GATILLO: Se dispara solo cuando AMBOS datasets se actualizan
    schedule=[DS_COTIZACIONES, DS_CSV_MENSUAL], 
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['etl', 'mensual', 'dbt'],
) as dag:


    run_dbt_build = BashOperator(
        task_id="run_dbt_build",
        bash_command='dbt build --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt'
    )

    run_dbt_build