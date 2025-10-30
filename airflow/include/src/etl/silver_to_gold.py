# %%
print('Importing libraries/functions and defining variables...')

from utils.bigquery import *
from utils.bucket import read_from_minio

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

from dotenv import load_dotenv
import os

load_dotenv()

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
project_id = "focus-storm-475900-p6"
dataset_name = "weather_lakehouse"
dataset_id = f"{project_id}.{dataset_name}"

bucket_name = 'weather-forecast-data-lake'
input_path = 'silver/'

today_str = datetime.now().strftime("%Y-%m-%d")

# %%
print('Initializing Spark session...')

spark = (
    SparkSession.builder.appName("Silver to Gold").getOrCreate()
)

# %%
print('Reading data:')

silver_ibge_cities = read_from_minio(
    spark=spark,
    bucket=bucket_name,
    path=f'{input_path}ibge/city/{today_str}',
    format='parquet'
)
silver_cptec_cities = read_from_minio(
    spark=spark,
    bucket=bucket_name,
    path=f'{input_path}cptec/city/{today_str}',
    format='parquet'
)
silver_cptec_weather = read_from_minio(
    spark=spark,
    bucket=bucket_name,
    path=f'{input_path}cptec/weather/{today_str}',
    format='parquet'
)
# %%
print('🥈 Silver tables schemas:\n')

print('📃 "silver_ibge_cities" table schema:')
silver_ibge_cities.printSchema()

print('📃 "silver_cptec_cities" table schema:')
silver_cptec_cities.printSchema()

print('📃 "silver_cptec_weather" table schema:')
silver_cptec_weather.printSchema()

# %%
print('🥇 Gold denormalized schemas:')

print('📃 "dim_city" table schema:')
dim_city = silver_ibge_cities.select(
    F.col('id').alias('id_ibge'),
    F.col('nome').alias('nome_ibge'),
    F.col('microrregiao_id'),
    F.col('microrregiao_nome'),
    F.col('regiao_imediata_id'),
    F.col('regiao_imediata_nome'),
    F.col('regiao_imediata_regiao_intermediaria_id').alias('id_regiao_intermediaria'),
    F.col('regiao_imediata_regiao_intermediaria_nome').alias('nome_regiao_intermediaria'),
    F.col('microrregiao_mesorregiao_UF_id').alias('uf_id'),
    F.col('microrregiao_mesorregiao_UF_sigla').alias('uf_sigla'),
    F.col('microrregiao_mesorregiao_UF_nome').alias('uf_nome')
)

dim_city = dim_city.join(
    silver_cptec_cities,
    silver_cptec_cities['nome'] == dim_city['nome_ibge'],
    how='inner'
).select(
    F.col('id_ibge'),
    F.col('id').alias('id_cptec'),
    F.coalesce(F.col('nome_ibge'), F.col('nome')).alias('nome'),
    'estado',
    'uf_sigla',
    'uf_nome',
    'microrregiao_id',
    'microrregiao_nome',
    'regiao_imediata_id',
    'regiao_imediata_nome',
    'id_regiao_intermediaria',
    'nome_regiao_intermediaria'
)

dim_city.printSchema()


print('📃 "dim_weather_condition" table schema:')
dim_weather_condition = silver_cptec_weather.select(
    F.monotonically_increasing_id().alias('id_condicao'),
    'condicao',
    'condicao_desc'
).dropDuplicates(['condicao', 'condicao_desc'])

dim_weather_condition.printSchema()


print('📃 "dim_date" table schema:')
dim_date = silver_cptec_weather.select(
    F.to_date('data', 'yyyy-MM-dd').alias('date')
).dropDuplicates().withColumn('date_id', F.monotonically_increasing_id())

dim_date = dim_date.withColumn('ano', F.year('date')) \
                    .withColumn('mes', F.month('date')) \
                    .withColumn('dia', F.dayofmonth('date')) \
                    .withColumn('dia_semana', F.date_format('date', 'E'))

dim_date.printSchema()


print('📃 "dim_uf" table schema:')
dim_uf_from_ibge = silver_ibge_cities.select(
    F.col('microrregiao_mesorregiao_UF_id').alias('uf_ibge_id'),
    F.col('microrregiao_mesorregiao_UF_sigla').alias('uf_sigla'),
    F.col('microrregiao_mesorregiao_UF_nome').alias('uf_nome')
).dropDuplicates(['uf_ibge_id', 'uf_sigla', 'uf_nome'])

dim_uf = dim_uf_from_ibge.select(
    F.col('uf_ibge_id').cast('long'),
    F.trim(F.col('uf_sigla')).alias('uf_sigla'),
    F.trim(F.col('uf_nome')).alias('uf_nome')
).dropDuplicates(['uf_ibge_id']) \
.withColumnRenamed('uf_ibge_id', 'id_uf')

dim_uf.printSchema()


print('📃 "fact_weather" table schema:')
fact_weather = silver_cptec_weather.alias('w') \
    .join(dim_city.alias('c'), F.col('w.cidade') == F.col('c.nome'), 'left') \
    .join(dim_weather_condition.alias('cond'), F.col('w.condicao') == F.col('cond.condicao'), 'left') \
    .join(dim_date.alias('d'), F.to_date('w.data', 'yyyy-MM-dd') == F.col('d.date'), 'left') \
    .join(dim_uf.alias('u'), F.col('w.estado') == F.col('u.uf_sigla'), 'left') \
    .select(
        F.monotonically_increasing_id().alias('id_fato'),
        F.col('d.date_id'),
        F.col('c.id_cptec').alias('id_cidade'),
        F.col('u.id_uf'),
        F.col('cond.id_condicao'),
        F.col('w.min').alias('temperatura_min'),
        F.col('w.max').alias('temperatura_max'),
        F.col('w.atualizado_em'),
        'w._source',
        'w._ingestion_date',
        'w._processing_date'
    )

fact_weather.printSchema()

# %%
print('Writing denormalized tables to BigQuery:')

create_dataset_if_not_exists(
    dataset_id=dataset_id,
    project_id=project_id,
)

write_bq_table(
    df=dim_city,
    project_id=project_id,
    dataset_name=dataset_name,
    table_name='dim_city'
)

write_bq_table(
    df=dim_date,
    project_id=project_id,
    dataset_name=dataset_name,
    table_name='dim_date'
)

write_bq_table(
    df=dim_uf,
    project_id=project_id,
    dataset_name=dataset_name,
    table_name='dim_uf'
)

write_bq_table(
    df=dim_weather_condition,
    project_id=project_id,
    dataset_name=dataset_name,
    table_name='dim_weather_condition'
)

write_bq_table(
    df=fact_weather,
    project_id=project_id,
    dataset_name=dataset_name,
    table_name='fact_weather'
)