# %%
# Defining variables and importing functions...
from utils.bigquery import write_bq_table, create_dataset_if_not_exists, upload_to_bq_once_a_year
from utils.data_ingestion import *
from datetime import datetime

import asyncio
from dotenv import load_dotenv
import os

load_dotenv()

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
project_id = "focus-storm-475900-p6"
dataset_name = "weather_lakehouse"
dataset_id = f"{project_id}.{dataset_name}"

today_str = datetime.now().strftime("%Y-%m-%d")

# %%
# Getting city base data
ibge_city_data = get_ibge_city_data()
city_names =  [i['nome'].replace('-', ' ') for i in ibge_city_data]

# %%
# Getting city data
city_data = asyncio.run(get_city_data(city_names))

# %%
city_ids = [city_data[i]['id'] for i in range(len(city_data))]
days = 6

# %%
city_ibge_ids = [i['id'] for i in ibge_city_data]

# %%
city_weather_data = asyncio.run(get_weather_data(city_ids, days=6))

# %%
# Minimally formatting data
import pandas as pd

# IBGE City Dataset
df_ibge_c = pd.DataFrame(ibge_city_data)

df_ibge_mr = pd.json_normalize(df_ibge_c['microrregiao'])
df_ibge_mr.columns = ['microrregiao_' + col.replace('.', '_') for col in df_ibge_mr.columns]

df_ibge_ri = pd.json_normalize(df_ibge_c['regiao-imediata'])
df_ibge_ri.columns = ['regiao_imediata_' + col.replace('.', '_') for col in df_ibge_ri.columns]

df_ibge_c = df_ibge_c[['id', 'nome']]
df_ibge_c = pd.concat([df_ibge_c, df_ibge_mr, df_ibge_ri], axis=1)
df_ibge_c['_source'] = 'IBGE API'
df_ibge_c['_ingestion_date'] = today_str


# CPTEC City Dataset
df_cptec_c = pd.DataFrame(city_data)
df_cptec_c['_source'] = 'CPTEC API'
df_cptec_c['_ingestion_date'] = today_str


# CPTEC City Weather Dataset
df_cptec_w = pd.DataFrame(city_weather_data)
df_cptec_w = df_cptec_w.explode('clima')
df_cptec_wc = pd.json_normalize(df_cptec_w['clima'])

df_cptec_w = pd.concat(
    [df_cptec_w[['cidade', 'estado', 'atualizado_em']].reset_index(drop=True),
    df_cptec_wc.reset_index(drop=True)],
    axis=1
)

df_cptec_w['_source'] = 'CPTEC API'
df_cptec_w['_ingestion_date'] = today_str

# %%
# Saving data into BigQuery
create_dataset_if_not_exists(dataset_id, project_id)

# IBGE City Dataset
upload_to_bq_once_a_year(df_ibge_c, dataset_id, "bronze_ibge_cities", project_id)

# CPTEC City Dataset
upload_to_bq_once_a_year(df_cptec_c, dataset_id, "bronze_cptec_cities", project_id)

# CPTEC City Weather Dataset
write_bq_table(
    df_cptec_w,
    project_id=project_id,
    dataset_name=dataset_name,
    table_name="bronze_cptec_weather"
)