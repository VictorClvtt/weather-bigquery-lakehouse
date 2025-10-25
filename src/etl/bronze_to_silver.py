# %%
print('Initializing Spark session...')
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.appName("Bronze to Silver").getOrCreate()

# %%
print('Importing environment variables...')
from dotenv import load_dotenv
import os

load_dotenv()

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

# %%
print('Defining functions and variables...')
project_id = 'focus-storm-475900-p6'
dataset_name = 'weather_lakehouse'

def read_bq_table(project_id, dataset_name, table_name, spark_session):
    from google.cloud import bigquery
    import pandas as pd
    from datetime import datetime

    # Get today's date in YYYY-MM-DD format
    today_str = datetime.now().strftime("%Y-%m-%d")

    # --- Basic parameter validation ---
    if not project_id or not dataset_name or not table_name:
        raise ValueError("❌ Missing required parameters: 'project_id', 'dataset_name', or 'table_name'.")
    if spark_session is None:
        raise ValueError("❌ 'spark_session' must be a valid SparkSession instance.")

    bq_table = f"{project_id}.{dataset_name}.{table_name}"

    print(f"\n🔍 Reading BigQuery table: {bq_table}")

    try:
        client = bigquery.Client(project=project_id)

        # Check if dataset exists
        try:
            client.get_dataset(dataset_name)
        except Exception:
            raise ValueError(f"❌ Dataset '{dataset_name}' not found in project '{project_id}'.")

        # Check if table exists
        try:
            client.get_table(bq_table)
        except Exception:
            raise ValueError(f"❌ Table '{bq_table}' not found in BigQuery.")

        if 'weather' in table_name:
            query = f"""
                SELECT *
                FROM `{bq_table}`
                WHERE DATE(_ingestion_timestamp) = '{today_str}'
            """
        elif 'cities' in table_name:
            query = f"""
                SELECT *
                FROM `{bq_table}`
                WHERE DATE(_ingestion_timestamp) = (
                    SELECT MAX(DATE(_ingestion_timestamp))
                    FROM `{bq_table}`
                )
            """

        print(f"🕒 Running query for {today_str} ...")

        df_pandas = client.query(query).to_dataframe()

        if df_pandas.empty:
            print(f"⚠️ No records found in '{bq_table}' for date {today_str}.")
        else:
            print(f"✅ Successfully read {len(df_pandas)} rows from '{bq_table}'.")

        # Convert to Spark DataFrame
        df_spark = spark_session.createDataFrame(df_pandas)
        print("✨ Data successfully converted to Spark DataFrame.")
        return df_spark

    except Exception as e:
        print(f"❌ Error reading table '{bq_table}': {e}")
        raise

def write_bq_table(df_spark, project_id, dataset_name, table_name, if_exists="append"):
    from google.cloud import bigquery
    import pandas as pd
    import gc

    print("\n🔄 Converting Spark DataFrame to Pandas...")
    df_pandas = df_spark.toPandas()

    client = bigquery.Client(project=project_id)
    table_id = f"{project_id}.{dataset_name}.{table_name}"

    # Set write mode
    if if_exists == "replace":
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    elif if_exists == "append":
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    else:
        raise ValueError("Parameter 'if_exists' must be 'replace' or 'append'.")

    print(f"📤 Uploading data to BigQuery table: {table_id} ...")

    try:
        load_job = client.load_table_from_dataframe(df_pandas, table_id, job_config=job_config)
        load_job.result()  # Wait for completion
        print(f"✅ BigQuery table '{table_id}' successfully written!")
    except Exception as e:
        print(f"❌ Error uploading to BigQuery: {e}")
        raise
    finally:
        # 🧹 Memory cleanup
        print("🧹 Releasing memory from DataFrames...")
        del df_pandas
        del df_spark
        gc.collect()
        print("✅ Memory successfully released!")

    return table_id

def remove_null_values(df):
    print('\nRemoving null values from each column:')
    
    for col, dtype in df.dtypes:
        if dtype not in ['string', 'date', 'timestamp']:
            null_count = df.filter(F.col(col).isNull() | F.isnan(F.col(col))).count()
            
            if null_count > 0:
                df = df.filter(~(F.col(col).isNull() | F.isnan(F.col(col))))
                print(f"✅ {col}: {null_count} null/NaN values removed.")
            else:
                print(f"☑️ {col}: No null/NaN values found.")
        else:
            null_count = df.filter(F.col(col).isNull()).count()
            
            if null_count > 0:
                df = df.filter(F.col(col).isNotNull())
                print(f"✅ {col}: {null_count} null values removed.")
            else:
                print(f"☑️ {col}: No null values found.")
    
    return df

def check_unique_values(df):
    print('\nChecking unique values from each column:')
    for col in df.columns:
        print(f'Unique value count: {df.select(F.col(col)).distinct().count()}')
        df.select(col).distinct().show(truncate=False)

def basic_data_profiling(df):
    print('\nBasic data Profiling:')
    for col, dtype in df.dtypes:
        print(f"\nColumn: {col}")
        df.select(
            F.count(F.col(col)).alias("count"),
            F.countDistinct(F.col(col)).alias("distinct"),
            F.min(F.col(col)).alias("min"),
            F.max(F.col(col)).alias("max")
        ).show(truncate=False)

def remove_whitespace(df):
    print('\nRemoving whitespace in values from each column:')

    string_columns = [col for col, dtype in df.dtypes if dtype == 'string']
    for col in string_columns:
        changed_count = df.filter(
            F.col(col) != F.trim(F.col(col))
        ).count()

        if changed_count > 0:
            df = df.withColumn(col, F.trim(F.col(col)))

            print(f"✅ {col}: {changed_count} lines trimmed")
        else:
            print(f"☑️ {col}: No lines to trim")
    return df

def drop_duplicates(df):
    print('\nDropping duplicate lines:')

    initial_count = df.count()
    df = df.dropDuplicates()
    final_count = df.count()

    dropped_lines = max(initial_count - final_count, 0)

    print(f"✅ {dropped_lines} duplicate lines dropped.")
    return df

def remove_columns(df, columns_to_remove: list):
    print('\nRemoving unnecessary columns:')

    for col in columns_to_remove:
        df = df.drop(col)
        print(f'✅ Column "{col}" removed successfully.')

    return df

# %%
print('\n📂 Working with the "bronze_cptec_weather" table:')
df_cptec_w = read_bq_table(
    project_id=project_id,
    dataset_name=dataset_name,
    table_name='bronze_cptec_weather',
    spark_session=spark
)

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
write_bq_table(
    df_spark=df_cptec_w,
    project_id=project_id,
    dataset_name=dataset_name,
    table_name='silver_cptec_weather'
)

print('\n📁 Finished working with the "bronze_cptec_weather" table.')


# %%
print('\n📂 Working with the "bronze_cptec_cities" table:')
df_cptec_c = read_bq_table(
    project_id=project_id,
    dataset_name=dataset_name,
    table_name='bronze_cptec_cities',
    spark_session=spark
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
write_bq_table(
    df_spark=df_cptec_c,
    project_id=project_id,
    dataset_name=dataset_name,
    table_name='silver_cptec_cities'
)

print('\n📁 Finished working with the "bronze_cptec_cities" table.')


# %%
print('\n📂 Working with the "bronze_ibge_cities" table:')
df_ibge_c = read_bq_table(
    project_id=project_id,
    dataset_name=dataset_name,
    table_name='bronze_ibge_cities',
    spark_session=spark
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
# df_ibge_c = remove_columns(df_ibge_c, [])

# %%
print('\nAdding metadata...')
df_ibge_c = df_ibge_c.withColumn("_processing_date", F.current_date())

# %%
write_bq_table(
    df_spark=df_ibge_c,
    project_id=project_id,
    dataset_name=dataset_name,
    table_name='silver_ibge_cities'
)

print('\n📁 Finished working with the "bronze_ibge_cities" table.')