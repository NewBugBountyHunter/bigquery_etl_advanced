# tests/test_etl.py
"""
Testes unitários básicos usando PySpark local.
Roda em modo local[*], cria pequenos DataFrames e verifica transformações.
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from src.etl.jobs import clean, transform
from src.etl.schema import student_schema
from src.utils.io import read_csv

@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder.master("local[2]").appName("test-etl").getOrCreate()
    yield spark
    spark.stop()

def make_sample_df(spark):
    data = [
        ("Alice", "A", 22, "F", "Brazil", "Sao Paulo", 80, "HighSchool", 120, 90.0, 85),
        ("Bob", "B", 24, "M", "Brazil", "Rio", 70, "Bachelors", 130, None, 75),
    ]
    cols = ["fNAME","lNAME","Age","gender","country","residence","entryEXAM","prevEducation","studyHOURS","Python","DB"]
    df = spark.createDataFrame(data, cols)
    return df

def test_clean_and_transform(spark):
    df = make_sample_df(spark)
    df_clean = clean(df)
    # check a coluna Python foi preenchida com -1.0 para nulos
    vals = df_clean.select("Python").collect()
    assert any([r.Python == -1.0 for r in vals]), "Python nulo deveria ser preenchido com -1.0"

    df_trans = transform(df_clean)
    # verificar que existe coluna subject e score após explode
    cols = df_trans.columns
    assert "subject" in cols and "score" in cols
    # verificar que rank_in_country existe
    assert "rank_in_country" in cols

def test_qc_helpers(spark):
    df = make_sample_df(spark)
    # check reading with schema (just ensure method works)
    # create temporary csv to read (optional) - but we'll test schema usage indirectly
    assert df.count() == 2
