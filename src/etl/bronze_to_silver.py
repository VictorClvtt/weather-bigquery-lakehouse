# %%
print('Initializing Spark session...')
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = (
    SparkSession.builder.appName("Bronze to Silver")
    .config(
        "spark.jars.packages",
        "org.apache.hadoop:hadoop-aws:3.4.1,com.amazonaws:aws-java-sdk-bundle:1.12.671"
    )
    .getOrCreate()
)

# %%
print('Defining variables and importing functions...')
from datetime import datetime

bucket_name = 'weather-forecast-data-lake'
input_path = 'bronze/'
output_path = 'silver/'

today_str = datetime.now().strftime("%Y-%m-%d")

from utils.bucket import *
from utils.data_cleaning import *
from utils.data_profiling import *

# %%
print('\n📂 Working with the "bronze_cptec_weather" table:')
df_cptec_w = read_from_minio(
    spark=spark,
    bucket=bucket_name,
    path=f'{input_path}cptec/weather/{today_str}.csv',
    format='csv'
)

df_cptec_w.show()
# %%
print('\nOriginal Schema:')
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
df_cptec_w = remove_null_values(df_cptec_w)
    
# %%
check_unique_values(df_cptec_w)

# %%
basic_data_profiling(df_cptec_w)

# %%
df_cptec_w = remove_whitespace(df_cptec_w)

# %%
df_cptec_w = drop_duplicates(df_cptec_w)

# %%
df_cptec_w = remove_columns(df_cptec_w, ['indice_uv'])

# %%
print('Adding metadata...')
df_cptec_w = df_cptec_w.withColumn("_processing_date", F.current_date())


# %%
write_to_minio(
    df=df_cptec_w,
    bucket=bucket_name,
    path=f'{output_path}cptec/weather/{today_str}',
    format='parquet'
)

print('\n📁 Finished working with the "bronze_cptec_weather" table.')


# %%
print('\n📂 Working with the "bronze_cptec_cities" table:')
df_cptec_c = read_from_minio(
    spark=spark,
    bucket=bucket_name,
    path=f'{input_path}cptec/city/{today_str}.csv',
    format='csv'
)

# %%
print('\nOriginal Schema:')
df_cptec_c.printSchema()

# %%        
df_cptec_c = remove_null_values(df_cptec_c)
    
# %%
check_unique_values(df_cptec_c)

# %%
basic_data_profiling(df_cptec_c)

# %%
df_cptec_c = remove_whitespace(df_cptec_c)

# %%
df_cptec_c = drop_duplicates(df_cptec_c)

# %%
print('\nChecking consistency between the columns "nome" and "id":')

inconsistencies = (
    df_cptec_c
    .groupBy("nome")
    .agg(F.countDistinct("id").alias("unique_ids"))
    .where(F.col("unique_ids") != 1)
)

if inconsistencies.count() > 0:
    print(f"⚠️ Found {inconsistencies.count()} inconsistent names with more than one ID.\n")
    inconsistencies.show(truncate=False)
else:
    print("✅ All names are consistently associated with a single ID.")

# %%
print('\nAdding metadata...')
df_cptec_c = df_cptec_c.withColumn("_processing_date", F.current_date())

# %%
write_to_minio(
    df=df_cptec_c,
    bucket=bucket_name,
    path=f'{output_path}cptec/city/{today_str}',
    format='parquet'
)

print('\n📁 Finished working with the "bronze_cptec_cities" table.')


# %%
print('\n📂 Working with the "bronze_ibge_cities" table:')
df_ibge_c = read_from_minio(
    spark=spark,
    bucket=bucket_name,
    path=f'{input_path}ibge/city/{today_str}.csv',
    format='csv'
)

# %%
print('\nOriginal Schema:')
df_ibge_c.printSchema()

new_cols = [col.replace('-', '_') for col in df_ibge_c.columns]

for old_name, new_name in zip(df_ibge_c.columns, new_cols):
    if old_name != new_name:
        df_ibge_c = df_ibge_c.withColumnRenamed(old_name, new_name)

print('Processed Schema:')
df_ibge_c.printSchema()

# %%
print('\nChecking consistency between the columns "nome" and "id":')

inconsistencies = (
    df_ibge_c
    .groupBy("nome")
    .agg(F.countDistinct("id").alias("unique_ids"))
    .where(F.col("unique_ids") != 1)
)

if inconsistencies.count() > 0:
    print(f"⚠️ Found {inconsistencies.count()} inconsistent names with more than one ID.\n")
    inconsistencies.show(truncate=False)
else:
    print("✅ All names are consistently associated with a single ID.")

# %%        
df_ibge_c = remove_null_values(df_ibge_c)
    
# %%
check_unique_values(df_ibge_c)

# %%
basic_data_profiling(df_ibge_c)

# %%
df_ibge_c = remove_whitespace(df_ibge_c)

# %%
df_ibge_c = drop_duplicates(df_ibge_c)

# %%
print('\nAdding metadata...')
df_ibge_c = df_ibge_c.withColumn("_processing_date", F.current_date())

# %%
write_to_minio(
    df=df_ibge_c,
    bucket=bucket_name,
    path=f'{output_path}ibge/city/{today_str}',
    format='parquet'
)

print('\n📁 Finished working with the "bronze_ibge_cities" table.')