# %%
# Importing libraries
import requests
import aiohttp
import asyncio
from asyncio import Semaphore

# %%
# Getting cities base data
def get_cities(uf='SP'):
    r = requests.get(f'https://servicodados.ibge.gov.br/api/v1/localidades/estados/{uf}/municipios')
    cities = r.json()
    print(f'☑️ {len(cities)} cities retrieved.')

    return cities

cities = get_cities()
city_names =  [i['nome'].replace('-', ' ') for i in cities]

# %%
# Getting city data
async def fetch_city(session, city_name):
    url = f'https://brasilapi.com.br/api/cptec/v1/cidade/{city_name}'
    try:
        async with session.get(url, timeout=5) as response:
            data = await response.json()
            if data:
                return city_name, data[0]
    except:
        pass
    return city_name, None

async def get_city_data(city_names):
    city_data = []
    city_names_length = len(city_names)

    print('### CITY DATA ###')

    async with aiohttp.ClientSession() as session:
        tasks = [fetch_city(session, city) for city in city_names]
        results = await asyncio.gather(*tasks)

        for index, (name, data) in enumerate(results, start=1):
            if data:
                city_data.append(data)
                print(f'✅ ({index}/{city_names_length}) - Data from "{name}" found.')
            else:
                print(f'❌ ({index}/{city_names_length}) - No data for "{name}".')

    print(f'☑️ Total Number of Entries: {len(city_data)}')
    return city_data

city_data = asyncio.run(get_city_data(city_names))

# %%
city_ids = [city_data[i]['id'] for i in range(len(city_data))]
days = 6

# %%
city_ibge_ids = [i['id'] for i in cities]

# %%
async def fetch_weather(session, sem, city_id, days):
    """Faz a requisição de previsão do tempo de uma cidade."""
    url = f'https://brasilapi.com.br/api/cptec/v1/clima/previsao/{city_id}/{days}'
    async with sem:  # controla o número de requisições simultâneas
        try:
            async with session.get(url, timeout=10) as response:
                if response.status == 200:
                    data = await response.json()
                    return city_id, data
                else:
                    return city_id, None
        except Exception:
            return city_id, None


async def get_weather_data(city_ids, days=1, max_concurrent=20):
    """Coleta dados meteorológicos de várias cidades de forma paralela."""
    city_weather_data = []
    city_ids_length = len(city_ids)
    sem = Semaphore(max_concurrent)  # limite de requisições simultâneas

    print('### CITY WEATHER DATA ###')

    async with aiohttp.ClientSession() as session:
        tasks = [fetch_weather(session, sem, city_id, days) for city_id in city_ids]
        results = await asyncio.gather(*tasks)

        for index, (city_id, data) in enumerate(results, start=1):
            if data:
                city_weather_data.append(data)
                print(f'✅ ({index}/{city_ids_length}) - Weather data for city id "{city_id}" found.')
            else:
                print(f'❌ ({index}/{city_ids_length}) - No data for city id "{city_id}".')

    print(f'☑️ Total Number of Entries: {len(city_weather_data)}')
    return city_weather_data

city_weather_data = asyncio.run(get_weather_data(city_ids, days=6))

# %%
# Minimally formatting data
import pandas as pd

# IBGE City Dataset
df_ibge_c = pd.DataFrame(cities)

df_ibge_mr = pd.json_normalize(df_ibge_c['microrregiao'])
df_ibge_mr.columns = ['microrregiao_' + col.replace('.', '_') for col in df_ibge_mr.columns]

df_ibge_ri = pd.json_normalize(df_ibge_c['regiao-imediata'])
df_ibge_ri.columns = ['regiao_imediata_' + col.replace('.', '_') for col in df_ibge_ri.columns]

df_ibge_c = df_ibge_c[['id', 'nome']]
df_ibge_c = pd.concat([df_ibge_c, df_ibge_mr, df_ibge_ri], axis=1)
df_ibge_c['_source'] = 'IBGE API'
df_ibge_c['_ingestion_timestamp'] = pd.Timestamp.now()


# CPTEC City Dataset
df_cptec_c = pd.DataFrame(city_data)
df_cptec_c['_source'] = 'CPTEC API'
df_cptec_c['_ingestion_timestamp'] = pd.Timestamp.now()


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
df_cptec_w['_ingestion_timestamp'] = pd.Timestamp.utcnow()

# %%
# Saving data into BigQuery
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
from dotenv import load_dotenv
import os

load_dotenv()

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
project_id = "focus-storm-475900-p6"
dataset_name = "weather_lakehouse"
dataset_id = f"{project_id}.{dataset_name}"

client = bigquery.Client(project=project_id)

def create_dataset_if_not_exists(dataset_id):
    try:
        client.get_dataset(dataset_id)
        print(f"Dataset {dataset_id} já existe.")
    except:
        dataset = bigquery.Dataset(dataset_id)
        dataset = client.create_dataset(dataset)
        print(f"Dataset {dataset_id} criado.")

create_dataset_if_not_exists(dataset_id)

def upload_to_bq(df, table_name):
    table_id = f"{dataset_id}.{table_name}"
    job = client.load_table_from_dataframe(df, table_id)
    job.result()
    print(f"✅ {len(df)} entries successfully recorded on {table_id}")

def upload_to_bq_once_a_year(df, table_name):
    table_id = f"{dataset_id}.{table_name}"
    current_year = pd.Timestamp.utcnow().year

    try:
        table = client.get_table(table_id)
        query = f"SELECT MAX(_ingestion_timestamp) AS last_date FROM `{table_id}`"
        result = client.query(query).to_dataframe()
        last_date = result['last_date'].iloc[0]

        if pd.notna(last_date) and pd.Timestamp(last_date).year == current_year:
            print(f"✅ {table_id} was already updated on {last_date.date()} — skipping annual upload.")
            return

        print(f"🔁 Updating {table_id} (last update: {last_date})...")
    except NotFound:
        print(f"🆕 Creating table {table_id} for the first time...")

    job = client.load_table_from_dataframe(df, table_id)
    job.result()
    print(f"✅ {len(df)} entries successfully recorded on {table_id}")


# IBGE City Dataset
upload_to_bq_once_a_year(df_ibge_c, "bronze_ibge_cities")

# CPTEC City Dataset
upload_to_bq_once_a_year(df_cptec_c, "bronze_cptec_cities")

# CPTEC City Weather Dataset
upload_to_bq(df_cptec_w, "bronze_cptec_weather")