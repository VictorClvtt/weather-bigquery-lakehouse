from pyspark.sql import SparkSession, DataFrame as SparkDataFrame
import pandas as pd
from io import BytesIO
import boto3
from botocore.exceptions import ClientError
from datetime import datetime, timedelta, timezone

def read_from_minio(
        spark: SparkSession,
        bucket: str,
        path: str,
        format: str = "parquet",
        endpoint: str = 'http://minio:9000'
):
    hadoop_conf = spark._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.endpoint", endpoint)
    hadoop_conf.set("fs.s3a.access.key", "admin")
    hadoop_conf.set("fs.s3a.secret.key", "admin123")
    hadoop_conf.set("fs.s3a.path.style.access", "true")
    hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    s3_path = f"s3a://{bucket}/{path}"
    print(f"📂 Reading data from: {s3_path}")

    if format == "csv":
        return spark.read.option("header", True).option("inferSchema", True).csv(s3_path)
    else:
        return spark.read.format(format).load(s3_path)




def write_to_minio(
    df,
    bucket: str,
    path: str,
    format: str = "parquet",
    mode: str = "overwrite",
    endpoint: str = "http://minio:9000",
    access_key: str = "admin",
    secret_key: str = "admin123",
):

    # 🧠 Caso seja um DataFrame Spark
    if isinstance(df, SparkDataFrame):
        spark = df.sparkSession
        spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", access_key)
        spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", secret_key)
        spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", endpoint)
        spark._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
        spark._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

        s3_path = f"s3a://{bucket}/{path}"
        print(f"💾 Writing Spark DataFrame to: {s3_path}")
        df.write.format(format).mode(mode).save(s3_path)
        print("✅ Data successfully written to MinIO via Spark!")

    # 🧠 Caso seja um DataFrame Pandas
    elif isinstance(df, pd.DataFrame):
        print(f"💾 Writing Pandas DataFrame to: s3://{bucket}/{path}")
        s3 = boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
        )

        buffer = BytesIO()
        if format == "parquet":
            df.to_parquet(buffer, index=False)
            content_type = "application/octet-stream"
        elif format == "csv":
            df.to_csv(buffer, index=False)
            content_type = "text/csv"
        elif format == "json":
            df.to_json(buffer, orient="records", lines=True)
            content_type = "application/json"
        else:
            raise ValueError(f"Formato não suportado: {format}")

        buffer.seek(0)
        s3.put_object(Bucket=bucket, Key=path, Body=buffer.getvalue(), ContentType=content_type)
        print("✅ Data successfully written to MinIO via boto3!")

    else:
        raise TypeError("O objeto deve ser um DataFrame Spark ou Pandas.")

def write_to_minio_once_a_year(
    df,
    bucket: str,
    path: str,
    format: str = "parquet",
    mode: str = "overwrite",
    endpoint: str = "http://minio:9000",
    access_key: str = "admin",
    secret_key: str = "admin123",
):
    """
    Só executa a escrita no MinIO se o objeto (arquivo) for mais antigo que 1 ano
    ou não existir ainda.
    """

    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )

    try:
        # 🕒 Verifica a data da última modificação do objeto
        response = s3.head_object(Bucket=bucket, Key=path)
        last_modified = response["LastModified"]

        # Converter para UTC e calcular diferença
        now = datetime.now(timezone.utc)
        delta = now - last_modified

        if delta < timedelta(days=365):
            print(f"⏳ Última inserção foi há {delta.days} dias — não será sobrescrita ainda.")
            return False  # não escreve novamente
        else:
            print(f"✅ Última inserção tem mais de 1 ano ({delta.days} dias). Regravando arquivo...")

    except s3.exceptions.ClientError as e:
        # Se o objeto não existe, cria pela primeira vez
        if e.response["Error"]["Code"] == "404":
            print("🆕 Objeto ainda não existe. Gravando pela primeira vez...")
        else:
            raise e

    write_to_minio(
        df=df,
        bucket=bucket,
        path=path,
        format=format,
        mode=mode,
        endpoint=endpoint,
        access_key=access_key,
        secret_key=secret_key,
    )

    return True

def create_bucket_if_not_exists(
    bucket_name: str,
    endpoint: str = "http://minio:9000",
    access_key: str = "admin",
    secret_key: str = "admin123",
):
    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )

    try:
        buckets = s3.list_buckets()
        existing_buckets = [b["Name"] for b in buckets.get("Buckets", [])]

        if bucket_name in existing_buckets:
            print(f"✅ Bucket '{bucket_name}' já existe.")
        else:
            s3.create_bucket(Bucket=bucket_name)
            print(f"🪣 Bucket '{bucket_name}' criado com sucesso!")

    except ClientError as e:
        print(f"❌ Erro ao criar/verificar bucket '{bucket_name}': {e}")
        raise e
