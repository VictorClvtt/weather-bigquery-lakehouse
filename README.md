# BigQuery Weather Report Data Warehouse
O projeto se trata de uma pipeline ETL automatizada que constrói e alimenta um data lake(camadas Bronze e Silver) e um data warehouse(camada Gold) com dados vindos das APIs do IBGE(dados sobre cidades) e da CPTEC(dados de previsão do tempo).

O projeto mistura elementos on-premises(executados localmente) como o a pipeline automatizada com Airflow e o bucket MinIO(data lake) com elementos em nuvem como o BigQuery e a Dashboard.

Diagrama do projeto:
![Diagrama do Projeto](.github/images/project_diagram.png)


## Etapas

### 1. [bronze_ingest.py](src/etl/bronze_ingest.py): Script de ingestão de dados.
O primeiro script é responsável por fazer requisições às APIs do IBGE e da CPTEC, a API do IBGE retorna dados basicos sobre todas as cidades do estado de São Paulo, enquanto a API da CPTEC retorna dados de previsão do tempo de até seis dias após a data da requisição a partir dos nomes das cidades retornados pela API do IBGE. Com os dados em memória o script faz uma pequena transformação que torna os dados tabulares para que sejam gravados em formato CSV na camada Bronze do data lake.

O script faz isso utilizando principalmente as bibliotecas requests para fazer as requisições, a biblioteca Pandas para mudar a estrutura dos dados e a biblioteca Boto3 para gravar os dados em formato CSV no data lake.

### 2. [bronze_to_silver.py](src/etl/bronze_to_silver.py): Script de limpeza e formatação de dados.
O segundo script é responsável por padronizar, procurar por inconsistências e defeitos nos dados vindos da camada Bronze e gravar os dados limpos em formato Parquet na camada Silver do data lake.

O script faz isso utilizando principalmente o framework PySpark que possibilita a leitura dos dados para um tipo de dados(DataFrame) que oferece diversos métodos e atributos que permitem explorar e processar os dados com facilidade e rapidez.

### 3. [silver_to_gold.py](src/etl/silver_to_gold.py): Script de modelagem de dados.
O terceiro script é responsável por criar um modelo dimensional de banco de dados com os dados vindos da camada Silver e gravar os dados na camada Gold do data warehouse no BigQuery.

O script faz isso também utilizando principalmente o framework PySpark para ler os dados e criar um modelo dimensional Star Schema com os dados da camada Silver para permitir que esses dados sejam consumidos de forma mais fácil e eficiente para fins analíticos.

### 4. [Airflow](airflow/): Automação e agendamento da pipeline.
Nessa etapa do projeto foi criada uma DAG com Airflow que automatiza e define as dependências entre as Tasks da pipeline.

O setup de todo o ambiente, tanto pro Airflow quanto pro Spark e pro MinIO foi feito utilizando Docker, o que permite a reproducibilidade do ambiente e minimiza erros de execução da pipeline.