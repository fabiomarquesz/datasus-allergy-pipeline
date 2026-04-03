from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os

# Caminho absoluto para o diretório do projeto
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
VENV_PYTHON = f"{PROJECT_ROOT}/venv/bin/python"

# Configurações da DAG
default_args = {
    'owner': 'fabio',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Definindo a DAG e seu agendamento
with DAG(
    'datasus_allergy_pipeline',
    default_args=default_args,
    description='Pipeline de ETL para dados de alergias do Datasus',
    schedule='@monthly',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['datasus', 'bronze', 'prata', 'ouro'],
) as dag:
    
    # Definindo as tarefas da DAG
    # Usando o BashOperator para simular comandos linux

    #Tarefa 1: Roda o script de extração via PySUS
    extract_task = BashOperator(
        task_id='extract_sih_data',
        bash_command=f'cd {PROJECT_ROOT} && {VENV_PYTHON} extraction/extract_sih.py',
    )

    # Tareta 2: Roda o script de ingestão para a camada bronze
    load_bronze_task = BashOperator(
        task_id='load_to_bronze',
        bash_command=f'cd {PROJECT_ROOT} && {VENV_PYTHON} extraction/load_to_bronze.py',
    )

    # Tarefa 3: Roda o script SQL da camada Prata dentro do Docker
    transform_silver_task = BashOperator(
        task_id='transform_to_silver',
        bash_command=f'docker exec datasus_postgres psql -U admin -d health_data -f /docker-entrypoint-initdb.d/01_dwh_transform.sql',
    )

    # Tarefa 4: Roda o script SQL da camada Ouro dentro do Docker
    transform_gold_task = BashOperator(
        task_id='transform_to_gold',
        bash_command=f"docker exec datasus_postgres psql -U admin -d health_data -f /docker-entrypoint-initdb.d/02_analytics.sql ",
    )

    # ORQUESTRAÇÃO (A Ordem de Execução)
    # Os sinais ">>" indicam a dependência. Uma tarefa só roda se a anterior brilhar verde!
    extract_task >> load_bronze_task >> transform_silver_task >> transform_gold_task