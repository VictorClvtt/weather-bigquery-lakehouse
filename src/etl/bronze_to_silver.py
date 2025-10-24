# %%
# Initializing Spark session
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.appName("Bronze to Silver").getOrCreate()

# %%
# Initializing environment variables
from dotenv import load_dotenv
import os

load_dotenv()

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

# %%
# Defining a function to read data from BigQuery
project_id = 'focus-storm-475900-p6'
dataset_name = 'weather_lakehouse'

def read_bq_table(project_id, dataset_name, table_name, spark_session):
    from google.cloud import bigquery
    import pandas as pd

    client = bigquery.Client()

    bq_table = f"{project_id}.{dataset_name}.{table_name}"

    query = f"""
        SELECT *
        FROM `{bq_table}`
        
    """

    df_pandas = client.query(query).to_dataframe()

    df_spark = spark.createDataFrame(df_pandas)

    return df_spark

# %%
# Reading CPTEC Weather data
df_cptec_w = read_bq_table(
    project_id=project_id,
    dataset_name=dataset_name,
    table_name='bronze_cptec_weather',
    spark_session=spark
)

# %%
print('Original Schema:')
df_cptec_w.printSchema()

df_cptec_w = df_cptec_w.withColumn(
    'atualizado_em',
    F.coalesce(
        F.to_date(F.col('atualizado_em'), 'yyyy-MM-dd'),
        F.to_date(F.col('atualizado_em'), 'dd/MM/yyyy'),
        F.to_date(F.col('atualizado_em'), 'MM-dd-yyyy')
    )
)

print('Processed Schema:')
df_cptec_w.printSchema()

# %%
print('Removing null values from each column:')
for col, dtype in df_cptec_w.dtypes:
    if dtype not in ['string', 'date', 'timestamp']:
        null_count = df_cptec_w.where(F.col(col).isNull() | F.col(col).isNaN()).count()
        
        if null_count > 0:
            df_cptec_w = df_cptec_w.where(~(F.col(col).isNull() | F.col(col).isNaN()))
            print(f"✅ {col}: {null_count} null/NaN values removed.")
        else:
            print(f"☑️ {col}: No null/NaN values found.")
    else:
        null_count = df_cptec_w.where(F.col(col).isNull()).count()

        if null_count > 0:
            df_cptec_w = df_cptec_w.where(F.col(col).isNotNull())
            print(f"✅ {col}: {null_count} null values removed.")
        else:
            print(f"☑️ {col}: No null values found.")
        

    
# %%
print('Checking unique values from each column:')
for col in df_cptec_w.columns:
    print(f'Unique value count: {df_cptec_w.select(F.col(col)).distinct().count()}')
    df_cptec_w.select(col).distinct().show(truncate=False)

# %%
print('Removing whitespace in values from each column:')
for col in [col for col, dtype in df_cptec_w.dtypes if dtype == 'string']:
    changed_count = df_cptec_w.filter(
        F.col(col) != F.trim(F.col(col))
    ).count()

    if changed_count > 0:
        df_cptec_w = df_cptec_w.withColumn(col, F.trim(F.col(col)))

        print(f"✅ {col}: {changed_count} lines trimmed")
    else:
        print(f"☑️ {col}: No lines to trim")