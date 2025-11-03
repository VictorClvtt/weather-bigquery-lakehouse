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
print('🥇 Gold denormalized schemas:\n')

##################
# CITY DIMENSION #
##################
print('📃 "dim_city" table schema:')
dim_city = silver_ibge_cities.select(
    F.col('id').alias('id_ibge'),
    F.col('nome').alias('nome_ibge'),
    F.col('microrregiao_mesorregiao_UF_regiao_nome').alias('nome_regiao'),
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
    'nome_regiao',
    'uf_sigla',
    'uf_nome',
)

dim_city = dim_city.withColumn(
    "id_city",
    F.sha2(F.concat_ws(":", F.col("id_ibge"), F.col("id_cptec")), 256)
)
dim_city.printSchema()

#########################
# UPDATE DATE DIMENSION #
#########################
print('📃 "dim_update_date" table schema:')
dim_update_date = silver_cptec_weather.select(F.col('atualizado_em').alias('data')).distinct()

dim_update_date = dim_update_date.withColumn(
    "id_update_date",
    F.sha2(F.col("data").cast("string"), 256)
)
dim_update_date.printSchema()

###########################
# FORECAST DATE DIMENSION #
###########################
print('📃 "dim_forecast_date" table schema:')
dim_forecast_date = silver_cptec_weather.select('data').distinct()

dim_forecast_date = dim_forecast_date.withColumn(
    "id_forecast_date",
    F.sha2(F.col("data").cast("string"), 256)
)
dim_forecast_date.printSchema()

###############################
# WEATHER CONDITION DIMENSION #
###############################
print('📃 "dim_weather_condition" table schema:')
dim_weather_condition = silver_cptec_weather.select('condicao', 'condicao_desc').distinct()

dim_weather_condition = dim_weather_condition.withColumn(
    "id_weather_condition",
    F.sha2(F.col("condicao"), 256)
)
dim_weather_condition.printSchema()

################
# WEATHER FACT #
################
print('📃 "fact_weather" table schema:')
fact_weather = silver_cptec_weather.alias('w') \
    .join(dim_city.alias('c'), F.col('w.cidade') == F.col('c.nome'), 'inner') \
    .select(
        F.col('w.min').alias('temperatura_min'),
        F.col('w.max').alias('temperatura_max'),
        'w.data',
        'w.atualizado_em',
        'w.condicao',
        'w._source',
        'w._ingestion_date',
        'w._processing_date',
        'c.id_city'
    )

fact_weather = fact_weather.withColumn(
    "id_update_date",
    F.sha2(F.col("atualizado_em").cast("string"), 256)
).withColumn(
    "id_forecast_date",
    F.sha2(F.col("data").cast("string"), 256)
).withColumn(
    "id_weather_condition",
    F.sha2(F.col("condicao"), 256)
).withColumn(
    '_modeling_date',
    F.lit(today_str)
).withColumn(
    "id_fact",
    F.sha2(
        F.concat_ws(
            "_",
            F.col("id_city"),
            F.col("id_forecast_date"),
            F.col("id_weather_condition")
        ),
        256
    )
)

fact_weather = fact_weather.select(
    'id_city',
    'id_update_date',
    'id_forecast_date',
    'id_weather_condition',
    'temperatura_min',
    'temperatura_max',
    '_source',
    '_ingestion_date',
    '_processing_date',
    '_modeling_date',
    'id_fact'
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
    df=dim_update_date,
    project_id=project_id,
    dataset_name=dataset_name,
    table_name='dim_update_date'
)

write_bq_table(
    df=dim_forecast_date,
    project_id=project_id,
    dataset_name=dataset_name,
    table_name='dim_forecast_date'
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