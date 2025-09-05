from __future__ import annotations

from airflow import DAG
from airflow.operators.bash import BashOperator
import pendulum
from datetime import timedelta
import os

with DAG(
    dag_id="pipeline_bancario",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule_interval="35 4 * * *",  # Executa todos os dias às 04:35 da manhã
    catchup=False,
    tags=["etl", "bancario", "data_lake"],
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    }
) as dag:
    
    # Define a pasta de scripts Python
    scripts_dir = "/opt/airflow/scripts"

    # Tarefa 2: Cria um único arquivo CSV a partir do dump SQL
    create_banvic_csv = BashOperator(
        task_id="create_banvic_csv",
        bash_command=f"python {scripts_dir}/3_criar_arquivo_banvic_csv.py",
    )

    # Tarefa 3: Extrai e salva os dados no formato de Data Lake
    # O arquivo 'banvic.csv' gerado na tarefa anterior é lido por este script
    extract_to_datalake = BashOperator(
        task_id="extract_to_datalake",
        bash_command=f"python {scripts_dir}/4_criar_diretorios_data_lake.py",
    )
    
    # Define a ordem de execução das tarefas
    create_banvic_csv >> extract_to_datalake