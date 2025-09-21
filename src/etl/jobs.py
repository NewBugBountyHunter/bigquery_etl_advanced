# src/etl/jobs.py
"""
Funções de limpeza e transformação em PySpark.
Projetado para ser importado e usado pelo seu main.py.
"""

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    row_number, avg, stddev, desc, array, struct, explode, lit, col, trim, lower, when
)

def clean(df):
    """
    Limpeza do DataFrame Spark:
    - trim em nomes
    - normalização de gender (m/Male -> Male; f/Female -> Female)
    - cast de tipos (Python -> Double, DB -> Integer)
    - corrigir studyHOURS negativo
    - preencher nulos de Python com -1.0 (marca de missing)
    """
    df2 = df.withColumn("fNAME", trim(col("fNAME"))) \
            .withColumn("lNAME", trim(col("lNAME"))) \
            .withColumn("gender",
                        when(lower(trim(col("gender"))).isin("m", "male"), lit("Male"))
                        .when(lower(trim(col("gender"))).isin("f", "female"), lit("Female"))
                        .otherwise(lit("Unknown"))) \
            .withColumn("Python", col("Python").cast(DoubleType())) \
            .withColumn("DB", col("DB").cast(IntegerType())) \
            .withColumn("studyHOURS", when(col("studyHOURS") < 0, lit(0)).otherwise(col("studyHOURS"))) \
            .na.fill({"Python": -1.0})
    return df2

def transform(df):
    """
    Transformações avançadas:
    - avg_score: média entre Python e DB
    - subjects: cria array de structs (subject, score)
    - explode para formato long (uma linha por estudante+disciplina)
    - ranking por country+subject com window function
    - cálculo de média, stddev e zscore por subject
    """
    df = df.withColumn("avg_score", (col("Python") + col("DB")) / 2.0)

    subjects_arr = array(
        struct(lit("Python").alias("subject"), col("Python").alias("score")),
        struct(lit("DB").alias("subject"), col("DB").alias("score"))
    )
    df = df.withColumn("subjects", subjects_arr)

    df_exploded = df.select(
        "fNAME", "lNAME", "country", "prevEducation", "avg_score",
        explode(col("subjects")).alias("sub")
    ).select(
        "fNAME", "lNAME", "country", "prevEducation", "avg_score",
        col("sub.subject").alias("subject"),
        col("sub.score").alias("score")
    )

    # ranking por país e disciplina
    w = Window.partitionBy("country", "subject").orderBy(desc("score"))
    df_ranked = df_exploded.withColumn("rank_in_country", row_number().over(w))

    # estatísticas por disciplina
    w2 = Window.partitionBy("subject")
    df_ranked = df_ranked.withColumn("mean_score", avg("score").over(w2)) \
                         .withColumn("stddev_score", stddev("score").over(w2)) \
                         .withColumn("zscore",
                                     (col("score") - col("mean_score")) / col("stddev_score"))

    return df_ranked
