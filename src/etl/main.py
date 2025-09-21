import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, lower, when, lit, regexp_replace,
    array, struct, explode, to_json, from_json
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType
)
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, avg, stddev, desc

def get_spark(app_name="student-etl"):
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()
    return spark

def read_input(spark, path):
    df = spark.read.option("header", "true") \
                   .option("inferSchema", "true") \
                   .option("encoding", "ISO-8859-1") \
                   .option("nullValue", "null") \
                   .csv(path)
    return df

def read_input_with_schema(spark, path):
    schema = StructType([
        StructField("fNAME", StringType(), True),
        StructField("lNAME", StringType(), True),
        StructField("Age", IntegerType(), True),
        StructField("gender", StringType(), True),
        StructField("country", StringType(), True),
        StructField("residence", StringType(), True),
        StructField("entryEXAM", IntegerType(), True),
        StructField("prevEducation", StringType(), True),
        StructField("studyHOURS", IntegerType(), True),
        StructField("Python", DoubleType(), True),
        StructField("DB", IntegerType(), True),
    ])
    df = spark.read.option("header", "true") \
                   .option("encoding", "ISO-8859-1") \
                   .schema(schema) \
                   .csv(path)
    return df

def clean(df):
    df2 = df.withColumn("fNAME", trim(col("fNAME"))) \
            .withColumn("lNAME", trim(col("lNAME"))) \
            .withColumn("gender", when(lower(trim(col("gender"))).isin("m","male"), lit("Male"))
                                   .when(lower(trim(col("gender"))).isin("f","female"), lit("Female"))
                                   .otherwise(lit("Unknown"))) \
            .withColumn("Python", col("Python").cast(DoubleType())) \
            .withColumn("DB", col("DB").cast(IntegerType())) \
            .withColumn("studyHOURS", when(col("studyHOURS") < 0, lit(0)).otherwise(col("studyHOURS"))) \
            .na.fill({"Python": -1.0}) 
    return df2

def transform(df):
    df = df.withColumn("avg_score", (col("Python") + col("DB")) / 2.0)

    subjects_arr = array(
        struct(lit("Python").alias("subject"), col("Python").alias("score")),
        struct(lit("DB").alias("subject"), col("DB").alias("score"))
    )
    df = df.withColumn("subjects", subjects_arr)

    df_exploded = df.select("fNAME", "lNAME", "country", "prevEducation", "avg_score", explode(col("subjects")).alias("sub")) \
                    .select("fNAME", "lNAME", "country", "prevEducation", "avg_score", col("sub.subject").alias("subject"),
                            col("sub.score").alias("score"))

    w = Window.partitionBy("country", "subject").orderBy(desc("score"))
    df_ranked = df_exploded.withColumn("rank_in_country", row_number().over(w))

    w2 = Window.partitionBy("subject")
    df_ranked = df_ranked.withColumn("mean_score", avg("score").over(w2)) \
                         .withColumn("stddev_score", stddev("score").over(w2)) \
                         .withColumn("zscore", (col("score") - col("mean_score")) / col("stddev_score"))

    return df_ranked

def write_output(df, out_path):
    df.write.mode("overwrite").partitionBy("country").parquet(out_path)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--use_schema", action="store_true")
    args = parser.parse_args()

    spark = get_spark()
    if args.use_schema:
        df = read_input_with_schema(spark, args.input)
    else:
        df = read_input(spark, args.input)

    df_clean = clean(df)
    df_transformed = transform(df_clean)
    write_output(df_transformed, args.output)

if __name__ == "__main__":
    main()
