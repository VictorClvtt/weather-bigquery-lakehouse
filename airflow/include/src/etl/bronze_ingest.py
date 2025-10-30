from .utils.bucket import write_to_minio, write_to_minio_once_a_year, create_bucket_if_not_exists
from .utils.data_ingestion import *
from datetime import datetime

import asyncio
from dotenv import load_dotenv
import os

def bronze_ingest():
    # %%
    # Defining variables...
    bucket_name = 'weather-forecast-data-lake'
    path = 'bronze/'

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
    # Saving data into the Data Lake
    create_bucket_if_not_exists(
        bucket_name=bucket_name,
    )

    # IBGE City Dataset
    write_to_minio_once_a_year(
        df=df_ibge_c,
        bucket=bucket_name,
        path=f'{path}ibge/city/{today_str}.csv',
        format='csv'
    )

    # CPTEC City Dataset
    write_to_minio_once_a_year(
        df=df_cptec_c,
        bucket=bucket_name,
        path=f'{path}cptec/city/{today_str}.csv',
        format='csv'
    )

    # CPTEC City Weather Dataset
    write_to_minio(
        df=df_cptec_w,
        bucket=bucket_name,
        path=f'{path}cptec/weather/{today_str}.csv',
        format='csv'
    )