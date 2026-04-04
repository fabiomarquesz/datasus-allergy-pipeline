from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os

# Caminhos dentro do container
PROJECT_ROOT = "/opt/airflow"

# Credenciais via variáveis de ambiente
PG_USER = os.environ.get("POSTGRES_USER", "admin")
PG_DB   = os.environ.get("POSTGRES_DB", "health_data")



# Configurações da DAG
default_args = {
    'owner': 'fabio',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'datasus_allergy_pipeline',
    default_args=default_args,
    description='Pipeline de ETL para dados de alergias do Datasus',
    schedule='@monthly',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['datasus', 'bronze', 'prata', 'ouro'],
) as dag:

    # Tarefa 1: Extração via PySUS
    extract_task = BashOperator(
        task_id='extract_sih_data',
        bash_command=f'cd {PROJECT_ROOT} && python extraction/extract_sih.py',
    )

    # Tarefa 2: Ingestão na camada Bronze
    load_bronze_task = BashOperator(
        task_id='load_to_bronze',
        bash_command=f'cd {PROJECT_ROOT} && python extraction/load_to_bronze.py',
    )

    # Tarefa 3: Transformação para camada Prata via SQL
    transform_silver_task = BashOperator(
    task_id='transform_to_silver',
    bash_command=f'cd {PROJECT_ROOT} && python extraction/transform_silver.py',
    ),

    # Dependências
    extract_task >> load_bronze_task >> transform_silver_task