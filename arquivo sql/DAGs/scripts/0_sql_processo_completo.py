from sqlalchemy import create_engine
from urllib.parse import quote_plus
from datetime import datetime
from io import StringIO
import pandas as pd
import psycopg2
import re
import os

"""1. cria as tabelas e suas relações importando do banvic.sql original; importa e alimenta as tabelas com os dados .csv 
de cada tabela (solução para problemas relacionados ao arquivo) """

def execute_idempotent_sql_script(connection_string, sql_file_path):
    """
    Lê, limpa, e executa um script SQL de um arquivo,
    tornando-o idempotente ao adicionar comandos DROP.
    """
    conn = None
    try:
        with open(sql_file_path, 'r') as file:
            full_script = file.read()

        cleaned_script = ""
        is_in_copy_block = False

        # Lógica para limpar o script de comandos específicos do pg_dump
        for line in full_script.splitlines():
            line = line.strip()
            if line.startswith('COPY') and 'FROM stdin' in line:
                is_in_copy_block = True
                continue
            if is_in_copy_block:
                if line == '\.':
                    is_in_copy_block = False
                continue

            if line.startswith('--') or line.startswith('SET ') or line.startswith('SELECT '):
                continue

            if line.startswith('\\'):
                continue

            cleaned_script += line + '\n'

        script_with_drops = ""
        for command in cleaned_script.split(';'):
            command = command.strip()
            if not command:
                continue

            match_type = re.search(r'CREATE TYPE public\."?(\w+)"?', command)
            match_table = re.search(r'CREATE TABLE public\."?(\w+)"?', command)

            if match_type:
                type_name = match_type.group(1)
                script_with_drops += f'DROP TYPE IF EXISTS public."{type_name}" CASCADE;\n'

            if match_table:
                table_name = match_table.group(1)
                script_with_drops += f'DROP TABLE IF EXISTS public."{table_name}" CASCADE;\n'

            script_with_drops += command + ';\n\n'

        sql_commands = [cmd.strip() for cmd in script_with_drops.split(';') if cmd.strip()]

        if not sql_commands:
            print("Erro: Nenhum comando SQL válido foi encontrado no arquivo após a limpeza. Por favor, verifique o conteúdo do arquivo.")
            return

        conn = psycopg2.connect(connection_string)
        cursor = conn.cursor()

        print("Conectado ao banco de dados. Executando script SQL...")

        for command in sql_commands:
            cursor.execute(command)

        conn.commit()

        print("Script SQL executado com sucesso!")

    except (Exception, psycopg2.Error) as error:
        print(f"Ocorreu um erro ao executar o script: {error}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            cursor.close()
            conn.close()
            print("Conexão com o banco de dados fechada.")

# --- Código Principal ---

try:
    password_raw = "v3rysecur&pas5w0rd"
    encoded_password = quote_plus(password_raw)
    connection_str = f"postgresql://data_engineer:{encoded_password}@localhost:5432/banvic"

    sql_file_path = '/content/banvic.sql'
    execute_idempotent_sql_script(connection_str, sql_file_path)

    engine = create_engine(connection_str)

    print("\nCarregando dados a partir dos arquivos CSV...")

    # Define os nomes das colunas para cada tabela na ordem correta
    column_names = {
        'agencias': ['cod_agencia', 'nome', 'endereco', 'cidade', 'uf', 'data_abertura', 'tipo_agencia'],
        'clientes': ['cod_cliente', 'primeiro_nome', 'ultimo_nome', 'email', 'tipo_cliente', 'data_inclusao', 'cpfcnpj', 'data_nascimento', 'endereco', 'cep'],
        'colaboradores': ['cod_colaborador', 'primeiro_nome', 'ultimo_nome', 'email', 'cpf', 'data_nascimento', 'endereco', 'cep'],
        'colaborador_agencia': ['cod_colaborador', 'cod_agencia'],
        'contas': ['num_conta', 'cod_cliente', 'cod_agencia', 'cod_colaborador', 'tipo_conta', 'data_abertura', 'saldo_total', 'saldo_disponivel', 'data_ultimo_lancamento'],
        'propostas_credito': ['cod_proposta', 'cod_cliente', 'cod_colaborador', 'data_entrada_proposta', 'taxa_juros_mensal', 'valor_proposta', 'valor_financiamento', 'valor_entrada', 'valor_prestacao', 'quantidade_parcelas', 'carencia', 'status_proposta']
    }

    # 5. Carrega os dados dos CSVs nas tabelas
    csv_files = {
        'agencias':'/content/public.agencias.csv',
        'clientes': '/content/public.clientes.csv',
        'colaboradores':'/content/public.colaboradores.csv',
        'colaborador_agencia':'/content/public.colaborador_agencia.csv',
        'contas': '/content/public.contas.csv',
        'propostas_credito': '/content/public.propostas_credito.csv'
    }

    for table_name, csv_file in csv_files.items():
        # Lê o CSV, passando os nomes das colunas
        df = pd.read_csv(csv_file, sep='\t', header=None, names=column_names[table_name])

        df.to_sql(table_name, engine, if_exists='append', index=False)
        print(f"Dados da tabela '{table_name}' carregados com sucesso.")

    print("\nTodos os dados foram carregados. Agora, rodando a consulta JOIN...")

    sql_query = """
    SELECT
        *
    FROM
        clientes AS c
    LEFT JOIN
        contas AS co ON c.cod_cliente = co.cod_cliente
    LEFT JOIN
        propostas_credito AS p ON c.cod_cliente = p.cod_cliente
    LEFT JOIN
        agencias AS a ON co.cod_agencia = a.cod_agencia
    LEFT JOIN
        colaboradores AS col ON co.cod_colaborador = col.cod_colaborador;
    """

    df = pd.read_sql_query(sql_query, engine)

    print("\n--- Resultado da Consulta ---")
    print(df.head())

except Exception as error:
    print(f"\nOcorreu um erro geral: {error}")

finally:
    if 'engine' in locals():
        engine.dispose()
    print("\nFim do processo.")

## --------------------------------------------------------------------------- ##
"""2. cria o CSV do banvic.sql"""

def get_dataframes_from_sql(sql_content):
    """
    Analisa o conteúdo SQL, extrai os dados de cada tabela e os
    converte em DataFrames do pandas com nomes de colunas únicos.
    """
    table_info = {
        'agencias': ['cod_agencia', 'nome_agencia', 'endereco_agencia', 'cidade_agencia', 'uf_agencia', 'data_abertura_agencia', 'tipo_agencia'],
        'clientes': ['cod_cliente', 'primeiro_nome', 'ultimo_nome', 'email', 'tipo_cliente', 'data_inclusao', 'cpfcnpj', 'data_nascimento', 'endereco', 'cep'],
        'colaboradores': ['cod_colaborador', 'primeiro_nome_colaborador', 'ultimo_nome_colaborador', 'email_colaborador', 'cpf', 'data_nascimento_colaborador', 'endereco_colaborador', 'cep_colaborador'],
        'contas': ['num_conta', 'cod_cliente', 'cod_agencia', 'cod_colaborador', 'tipo_conta', 'data_abertura_conta', 'saldo_total', 'saldo_disponivel', 'data_ultimo_lancamento'],
        'propostas_credito': ['cod_proposta', 'cod_cliente', 'cod_colaborador_proposta', 'data_entrada_proposta', 'taxa_juros_mensal', 'valor_proposta', 'valor_financiamento', 'valor_entrada', 'valor_prestacao', 'quantidade_parcelas', 'carencia', 'status_proposta']
    }

    dfs = {}
    data_pattern = re.compile(r"COPY public\.(\w+) \(.+?\) FROM stdin;\n(.+?)\n\\\.", re.DOTALL)
    matches = data_pattern.finditer(sql_content)

    for match in matches:
        table_name = match.group(1)
        data_string = match.group(2)

        if table_name in table_info:
            columns = table_info[table_name]
            data_io = StringIO(data_string)
            df = pd.read_csv(data_io, sep='\t', header=None, names=columns)
            dfs[table_name] = df
            print(f"Dados da tabela '{table_name}' extraídos com sucesso.")

    return dfs

try:
    with open('banvic.sql', 'r') as file:
        sql_content = file.read()

    dataframes = get_dataframes_from_sql(sql_content)

    # 1. Inicia o DataFrame combinado com a tabela de clientes
    df_completo = dataframes['clientes'].copy()

    # 2. Junta as tabelas em uma sequência lógica
    print("\nRealizando as junções (JOINs) para combinar os dados...")

    # Junta com a tabela de contas
    df_completo = df_completo.merge(dataframes['contas'], on='cod_cliente', how='left')

    # Junta com a tabela de propostas de crédito
    df_completo = df_completo.merge(dataframes['propostas_credito'], on='cod_cliente', how='left')

    # Junta com a tabela de agências
    df_completo = df_completo.merge(dataframes['agencias'], on='cod_agencia', how='left')

    # Junta com a tabela de colaboradores
    df_completo = df_completo.merge(dataframes['colaboradores'], on='cod_colaborador', how='left')

    # 3. Seleciona e organiza as colunas únicas e relevantes para o arquivo final
    final_columns = [
        'cod_cliente', 'primeiro_nome', 'ultimo_nome', 'email', 'tipo_cliente', 'data_inclusao', 'cpfcnpj', 'data_nascimento', 'endereco', 'cep',
        'num_conta', 'cod_agencia', 'cod_colaborador', 'tipo_conta', 'data_abertura_conta', 'saldo_total', 'saldo_disponivel', 'data_ultimo_lancamento',
        'cod_proposta', 'data_entrada_proposta', 'taxa_juros_mensal', 'valor_proposta', 'valor_financiamento', 'valor_entrada', 'valor_prestacao',
        'quantidade_parcelas', 'carencia', 'status_proposta',
        'nome_agencia', 'endereco_agencia', 'cidade_agencia', 'uf_agencia', 'tipo_agencia'
    ]

    # Faz uma seleção final das colunas e as organiza
    df_completo = df_completo.reindex(columns=final_columns)

    # 4. Salva o DataFrame final em um arquivo CSV
    output_filename = "banvic.csv"
    df_completo.to_csv(output_filename, index=False)

    print(f"\nDataFrame final criado com {len(df_completo.columns)} colunas.")
    print("As primeiras 5 linhas do DataFrame:")
    print(df_completo.head())
    print(f"\nDados salvos com sucesso no arquivo: '{output_filename}'")
    print(f"O arquivo '{output_filename}' agora está disponível para download.")

except FileNotFoundError:
    print("Erro: O arquivo 'banvic.sql' não foi encontrado. Por favor, verifique se o nome do arquivo está correto e se ele foi carregado no ambiente.")
except Exception as e:
    print(f"\nOcorreu um erro geral: {e}")

## --------------------------------------------------------------------------- ##
"""3. Cria os diretórios com base na coluna data_ultima_movientacao """

código reciclado do CSV
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
    csv_source = "/content/banvic.csv"

    # Caminho base do Data Lake
    data_lake_path = "datalake_bruto"

    # Executa a função
    extract_and_save_sql_by_date(csv_source, data_lake_path)