import pandas as pd
import os
#código reciclado do CSV
def extract_and_save_sql_by_date(source_file_path, base_output_path):
    """
    Extrai dados de um arquivo CSV, agrupa-os pela coluna 'data_transacao'
    e salva cada grupo em uma estrutura de diretórios baseada na data da transação.

    Args:
        source_file_path (str): O caminho completo para o arquivo CSV de origem.
        base_output_path (str): O caminho base para o diretório de saída.
    """

    # --- 1. Extração dos dados e tratamento de erros
    try:
        df = pd.read_csv(source_file_path)
        print(f"Dados extraídos com sucesso do arquivo: {source_file_path}")
    except FileNotFoundError:
        print(f"Erro: O arquivo de origem não foi encontrado em: {source_file_path}")
        return
    except Exception as e:
        print(f"Ocorreu um erro ao ler o arquivo CSV: {e}")
        return

    # --- 2. Preparação da coluna de data
    df['data_ultimo_lancamento'] = pd.to_datetime(df['data_ultimo_lancamento'], utc=True, format="mixed")

    df.dropna(subset=['data_ultimo_lancamento'], inplace=True)

    df['apenas_data'] = df['data_ultimo_lancamento'].dt.date

    # --- 3. Agrupamento e salvamento dos dados por data
    grouped_by_date = df.groupby('apenas_data')

    for transaction_date, group_df in grouped_by_date:
        ano = transaction_date.strftime("%Y")
        mes = transaction_date.strftime("%m")
        dia = transaction_date.strftime("%d")

        source_name = "fonte_sql"
        output_dir = os.path.join(base_output_path, ano, mes, dia, source_name)

        output_file_path = os.path.join(output_dir, "banvic_sql.csv")

        os.makedirs(output_dir, exist_ok=True)
        print(f"Diretório de saída garantido: {output_dir}")

        try:
            group_df.to_csv(output_file_path, index=False)
            print(f"Dados da data {transaction_date} salvos em: {output_file_path}")
        except Exception as e:
            print(f"Ocorreu um erro ao salvar o arquivo para a data {transaction_date}: {e}")

if __name__ == "__main__":
    # Caminho do arquivo CSV de origem
    csv_source = "/opt/airflow_projects/data/banvic.csv"

    # Caminho base do Data Lake
    data_lake_path = "/opt/airflow/data/datalake_bruto"

    # Executa a função
    extract_and_save_sql_by_date(csv_source, data_lake_path)