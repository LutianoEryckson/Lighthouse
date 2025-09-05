# Proj. Eng. de Dados

# **Desafio**

*Neste desafio você deverá realizar a extração e carregamento dos dados de ambas as fontes fornecidas (CSV e SQL) para um Data Warehouse **local** (PostgreSQL). Essa tarefa deverá respeitar os requisitos e diagrama abaixo:*

![image.png](image.png)

### **Requisitos**

- Utilize o Apache Airflow (2 ou 3) como orquestrador de tarefas; - instalar no WSL
- As extrações devem ser idempotentes → ou seja, o código tem que retornar a mesma saída todas as vezes.
- Devem ser extraídos todos os dados fornecidos;
- As extrações devem escrever os dados no formato CSV para seu FIleSystem Local seguindo o padrão de nomenclatura:
    - <ano>-<mês>-<dia>/<fonte-de-dados>/<nome-da-tabela-ou-csv>.csv
- As etapas de extração de dados devem ocorrer uma em paralelo à outra;
- A etapa de carregamento no Data Warehouse deve ocorrer somente se ambas extrações tenham sucesso;
- O pipeline deve ser executado todos os dias às 04:35 da manhã;
- O projeto deve ser reproduzível em outros ambientes.

# **Entregas**

1. Todos os códigos e configurações utilizados para a execução do projeto devem ser compactados num arquivo **.zip** e anexados na entrega.
2. Um documento descritivo com instruções para execução do projeto em outros ambientes.
3. Um **vídeo** curto explicando a lógica utilizada em sua DAG do Airflow bem como uma execução manual da mesma e os resultados obtidos a partir da execução do pipeline (arquivos escritos no formato desejado e dados adicionados ao Data Warehouse).

ATÉ QUINTA FEIRA

# Notas pessoais sobre o projeto

## 1. Arquivo CSV - transacoes.csv

A coluna de datas que foi a base para a construção dos diretórios possui datas com formato misto → alguns registros tem milissegundos, outras não. O que impedia a manipulação inicial para criação dos diretórios. 

Corrigido com o:

```python
df['data_transacao'] = pd.to_datetime(df['data_transacao'], utc=True, format="mixed")
```

A partir disso, os diretórios recursivos foram criados sem problemas. 

## 2. Arquivo SQL - banvic.sql

O trabalho que o .csv não deu, esse deu de sobra. 

1. Não consegui importar os dados direto do arquivo .sql; provavelmente algum conflito da minha versão do Postgres Vs. a versão do banco “original”.
2. Fuçando o arquivo, vi que dentre os dados dos clientes, havia um endereço com ‘aspas simples’, o que deixava um bloco dos dados como ‘string’.

![image.png](image%201.png)

Um bloco bem grandinho diga-se de passagem. 

1. A senha com caractere especial

O sqlalchemy, lib usada para manipular esses dados, não conseguia interpretar a senha por causa do *‘&’*. Resolvido com: 

```python
#imbutido nos códigos
password_raw = "v3rysecur&pas5w0rd"
encoded_password = quote_plus(password_raw)
connection_str = f"postgresql://data_engineer:{encoded_password}@localhost:5432/banvic"

conn = psycopg2.connect(connection_str)
```

1. A forma como foi feito o import dos dados para o DB

A utilidade do arquivo .sql foi basicamente pra “restaurar” a estrutura das tabelas e suas relações. Os dados dos arquivos foram importados de .csvs correspondentes as mesmas.

![image.png](image%202.png)

![image.png](image%203.png)

Ao todo, são 46 colunas. Quando “mergedas”, 33. Estão organizadas por ordem de relevância.

![image.png](image%204.png)

1. A criação dos diretórios

Uma vez que chegou-se ao DB com as tabelas/colunas, foi xuxu beleza pra criar os diretórios. Bastou reutilizar o código usado pro CSV e adptar para o SQL (que na verdade agora é um CSV também 😛).

![image.png](image%205.png)

Diferente do CSV, o banvic.sql tem 5 colunas de datas: *data_inclusao, data_nascimento, data_abertura_conta, data_entrada_proposta* e **data_ultimo_lancamento**, sendo essa última a utilizada para a criação dos diretórios. Era a de maior peso para a construção dos mesmos.