import requests
import aiohttp
import asyncio
from asyncio import Semaphore

def get_ibge_city_data(uf='SP'):
    r = requests.get(f'https://servicodados.ibge.gov.br/api/v1/localidades/estados/{uf}/municipios')
    cities = r.json()
    print(f'☑️ {len(cities)} cities retrieved.')

    return cities

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