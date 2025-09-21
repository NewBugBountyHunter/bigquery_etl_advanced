# src/utils/io.py
"""
Helpers de leitura/escrita com PySpark.
"""

from pyspark.sql import SparkSession
from typing import Optional, Any
from pyspark.sql import DataFrame

def spark_session(app_name: str = "student-etl-local") -> SparkSession:
    """
    Cria/retorna SparkSession padrão para execução local.
    """
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()
    return spark

def read_csv(spark: SparkSession, path: str, schema: Optional[Any] = None) -> DataFrame:
    """
    Lê CSV com opções recomendadas (header, encoding). Se schema for fornecido, usa .schema(schema).
    """
    reader = spark.read.option("header", "true").option("encoding", "ISO-8859-1")
    if schema is not None:
        reader = reader.schema(schema)
    else:
        reader = reader.option("inferSchema", "true")
    df = reader.csv(path)
    return df

def write_parquet(df: DataFrame, out_path: str, partition_by: list = None):
    """
    Escreve DataFrame em parquet, modo overwrite por padrão.
    Partition_by aceita lista (ex: ["country"])
    """
    if partition_by:
        df.write.mode("overwrite").partitionBy(*partition_by).parquet(out_path)
    else:
        df.write.mode("overwrite").parquet(out_path)
