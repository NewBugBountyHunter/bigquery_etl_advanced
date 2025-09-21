# src/etl/schema.py
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# Schema explícito para leitura segura do CSV (use com read.schema(student_schema))
student_schema = StructType([
    StructField("fNAME", StringType(), True),
    StructField("lNAME", StringType(), True),
    StructField("Age", IntegerType(), True),            # note: "Age" capitalizado conforme seu CSV original
    StructField("gender", StringType(), True),
    StructField("country", StringType(), True),
    StructField("residence", StringType(), True),
    StructField("entryEXAM", IntegerType(), True),
    StructField("prevEducation", StringType(), True),
    StructField("studyHOURS", IntegerType(), True),
    StructField("Python", DoubleType(), True),
    StructField("DB", IntegerType(), True),
])
