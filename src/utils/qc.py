# src/utils/qc.py
"""
Quality checks simples para DataFrame Spark.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def check_not_empty(df: DataFrame):
    """
    Gera erro se DataFrame estiver vazio.
    """
    cnt = df.count()
    if cnt == 0:
        raise AssertionError("DataFrame está vazio!")

def count_nulls(df: DataFrame) -> dict:
    """
    Retorna dicionário coluna -> nulos
    """
    exprs = [F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df.columns]
    row = df.select(*exprs).collect()[0].asDict()
    return row

def has_columns(df: DataFrame, required_cols: list):
    """
    Valida presença de colunas; lança erro se faltar alguma.
    """
    cols = df.columns
    missing = [c for c in required_cols if c not in cols]
    if missing:
        raise AssertionError(f"Faltam colunas: {missing}")
