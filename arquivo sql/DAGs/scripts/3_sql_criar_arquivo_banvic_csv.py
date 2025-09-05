import pandas as pd
import re
from io import StringIO

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
    with open('/opt/airflow_projects/data/banvic.sql', 'r') as file:
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

    # 3. Seleciona e organiza as 33 colunas únicas e relevantes para o arquivo final
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