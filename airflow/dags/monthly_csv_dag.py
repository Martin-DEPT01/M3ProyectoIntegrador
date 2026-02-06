from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.datasets import Dataset
from datetime import datetime

# 1. Definimos el Dataset que este DAG "produce"
# Este mismo nombre debe usar el DAG 3 en su 'schedule'
DS_CSV_MENSUAL = Dataset("mysql://rds/csv_raw_mensual")



with DAG(
    dag_id="dag2_mensual_csv",
    description="Extrae los alquileres desde la carpeta DATA, carga el csv en S3 y luego carga en MySQL RDS capa raw",
    schedule="@monthly",           # Se ejecuta el primer día de cada mes a las 00:00
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['elt', 'mensual', 'csv'],
) as dag:


    run_csv_raw = BashOperator(
        task_id="run_csv_raw",
        bash_command='python /opt/airflow/scripts/csv_raw.py'
    )

    run_csv_elt = BashOperator(
        task_id="run_csv_elt",
        bash_command='python /opt/airflow/scripts/csv_elt.py',
        outlets=[DS_CSV_MENSUAL] # <--- Gatillo para el DAG 3
    )

    run_csv_raw >> run_csv_elt