import pandas as pd
import os
from datetime import datetime

def extract_and_save_csv_by_date(source_file_path, base_output_path):
    """
    Extrai dados de um arquivo CSV, agrupa-os pela coluna 'data_transacao'
    e salva cada grupo em uma estrutura de diretórios baseada na data da transação.

    Args:
        source_file_path (str): O caminho completo para o arquivo CSV de origem.
        base_output_path (str): O caminho base para o diretório de saída.
    """

    # --- 1. Extração dos dados e tratamento de erros
    try:
        # Lê o arquivo CSV completo para processamento
        df = pd.read_csv(source_file_path)
        print(f"Dados extraídos com sucesso do arquivo: {source_file_path}")
    except FileNotFoundError:
        print(f"Erro: O arquivo de origem não foi encontrado em: {source_file_path}")
        return
    except Exception as e:
        print(f"Ocorreu um erro ao ler o arquivo CSV: {e}")
        return

    # --- 2. Preparação da coluna de data
    # Converte a coluna 'data_transacao' para o tipo datetime.
    # format='mixed' -> corrige a questão de ter datas com/sem milessegundos.
    df['data_transacao'] = pd.to_datetime(df['data_transacao'], utc=True, format="mixed")

    # Após a conversão, removemos qualquer linha que tenha um valor de data inválido.
    df.dropna(subset=['data_transacao'], inplace=True)

    # Extrai apenas a data da coluna 'data_transacao' e cria uma nova coluna para o agrupamento
    df['apenas_data'] = df['data_transacao'].dt.date

    # --- 3. Agrupamento e salvamento dos dados por data
    # Agrupa o DataFrame por cada data única encontrada na nova coluna 'apenas_data'
    grouped_by_date = df.groupby('apenas_data')

    # Itera sobre cada grupo (cada data)
    for transaction_date, group_df in grouped_by_date:
        # Extrai ano, mês e dia
        ano = transaction_date.strftime("%Y")
        mes = transaction_date.strftime("%m")
        dia = transaction_date.strftime("%d")

        # Constrói o caminho completo do diretório de saída
        source_name = "fonte_csv"
        output_dir = os.path.join(base_output_path, ano, mes, dia, source_name)

        # O nome do arquivo final é 'transacoes.csv'
        output_file_path = os.path.join(output_dir, "transacoes.csv")

        # Cria os diretórios de forma recursiva. O parâmetro exist_ok=True
        # garante que a função não falhe se o diretório já existir.
        os.makedirs(output_dir, exist_ok=True)
        print(f"Diretório de saída garantido: {output_dir}")

        # Salva o sub-DataFrame (o grupo de transações de um dia) no novo arquivo CSV
        try:
            group_df.to_csv(output_file_path, index=False)
            print(f"Dados da data {transaction_date} salvos em: {output_file_path}")
        except Exception as e:
            print(f"Ocorreu um erro ao salvar o arquivo para a data {transaction_date}: {e}")

if __name__ == "__main__":
    # Caminho do arquivo CSV de origem
    csv_source = "/content/transacoes.csv"

    # Caminho base do Data Lake
    data_lake_path = "datalake_bruto"

    # Executa a função
    extract_and_save_csv_by_date(csv_source, data_lake_path)