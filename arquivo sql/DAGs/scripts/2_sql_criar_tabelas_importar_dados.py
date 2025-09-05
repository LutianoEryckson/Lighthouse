import re
import pandas as pd
import psycopg2
from urllib.parse import quote_plus
from sqlalchemy import create_engine

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
    connection_str = f"postgresql://data_engineer:{encoded_password}@localhost:55432/banvic"

    sql_file_path = '/opt/airflow_projects/data/banvic.sql'
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
        'agencias':'/opt/airflow_projects/data/public.agencias.csv',
        'clientes': '/opt/airflow_projects/data/public.clientes.csv',
        'colaboradores':'/opt/airflow_projects/data/public.colaboradores.csv',
        'colaborador_agencia':'/opt/airflow_projects/data/public.colaborador_agencia.csv',
        'contas': '/opt/airflow_projects/data/public.contas.csv',
        'propostas_credito': '/opt/airflow_projects/data/public.propostas_credito.csv'
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