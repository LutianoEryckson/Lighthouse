from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id='meu_pipeline_de_dados',
    default_args=default_args,
    schedule_interval='35 4 * * *',  # Executa todos os dias às 04:35 da manhã
    start_date=days_ago(1),
    catchup=False,
    tags=['dados', 'extração', 'carregamento'],
)
def meu_pipeline_de_dados():
    """
    Um pipeline de dados para extração e carregamento.
    """

    # Define a pasta de destino para os arquivos CSV
    csv_dir = '/opt/airflow/dags/files'  # Caminho dentro do contêiner Airflow
    os.makedirs(csv_dir, exist_ok=True)

    # Tarefa de extração 1 - Supondo que você use um script Python
    @task(task_id='extrair_dados_clientes')
    def extrair_clientes():
        # Código para extração de clientes
        print("Extraindo dados de clientes...")
        # Exemplo de escrita de arquivo local
        with open(os.path.join(csv_dir, 'clientes.csv'), 'w') as f:
            f.write('id,nome\n1,joão\n2,maria')
        return "Extração de clientes concluída."

    # Tarefa de extração 2
    @task(task_id='extrair_dados_pedidos')
    def extrair_pedidos():
        # Código para extração de pedidos
        print("Extraindo dados de pedidos...")
        with open(os.path.join(csv_dir, 'pedidos.csv'), 'w') as f:
            f.write('id,produto\n1,livro\n2,caneta')
        return "Extração de pedidos concluída."

    # Tarefa de carregamento
    # Garante que as extrações de clientes e pedidos sejam concluídas antes de começar
    carregar_dados_dw = BashOperator(
        task_id='carregar_dados_no_dw',
        bash_command=f'''
            echo "Carregando dados no Data Warehouse..."
            ls -l {csv_dir}
            # Lógica para carregar os arquivos CSV no DW
            echo "Carregamento concluído."
        '''
    )

    # Define a ordem de execução
    # As extrações ocorrem em paralelo
    # O carregamento só ocorre após o sucesso de ambas
    [extrair_clientes(), extrair_pedidos()] >> carregar_dados_dw

# Instancia a DAG
meu_pipeline_de_dados()