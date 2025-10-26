from google.cloud import bigquery
from google.api_core.exceptions import NotFound
import gc
import pandas as pd
from datetime import datetime

def create_dataset_if_not_exists(dataset_id, project_id=None, location="US"):
    client = bigquery.Client(project=project_id)

    try:
        client.get_dataset(dataset_id)
        print(f"✅ Dataset '{dataset_id}' já existe.")
    except Exception:
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = location
        client.create_dataset(dataset)
        print(f"🚀 Dataset '{dataset_id}' criado com sucesso ({location}).")

def upload_to_bq_once_a_year(df, dataset_id, table_name, project_id):
    table_id = f"{dataset_id}.{table_name}"
    current_year = pd.Timestamp.utcnow().year
    client = bigquery.Client(project=project_id)

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

def read_bq_table(project_id, dataset_name, table_name, spark_session):
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

def write_bq_table(df, project_id, dataset_name, table_name, if_exists="append"):
    print("\n🔄 Converting DataFrame to Pandas...")
    if type(df) != pd.DataFrame:
        df_pandas = df.toPandas()
    else:
        df_pandas = df

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
        del df
        gc.collect()
        print("✅ Memory successfully released!")

    return table_id
