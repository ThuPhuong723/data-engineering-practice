from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import zipfile
import os

def create_spark_session():
    return SparkSession.builder.appName("HardDriveFailureAnalysis").getOrCreate()

def extract_zip_data(zip_path='data/hard-drive-2022-01-01-failures.csv.zip', extract_to='data/extracted'):
    os.makedirs(extract_to, exist_ok=True)
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_to)

def load_data_with_filename(spark, data_dir='data/extracted'):
    return spark.read.option("header", True).csv(f"{data_dir}/*.csv", inferSchema=True)\
              .withColumn("source_file", input_file_name())

def extract_file_date(df):
    return df.withColumn(
        "file_date",
        regexp_extract("source_file", r"(\d{4}-\d{2}-\d{2})", 1).cast("date")
    )

def extract_brand(df):
    return df.withColumn(
        "brand",
        when(instr(col("model"), " ") > 0, split(col("model"), " ").getItem(0)).otherwise(lit("unknown"))
    )

def add_storage_ranking(df):
    model_capacity = df.groupBy("model").agg(avg("capacity_bytes").alias("avg_capacity"))
    window_spec = Window.orderBy(desc("avg_capacity"))
    ranked = model_capacity.withColumn("storage_ranking", dense_rank().over(window_spec))
    return df.join(ranked.select("model", "storage_ranking"), on="model", how="left")

def add_primary_key(df):
    concat_cols = concat_ws("||", *df.columns)
    return df.withColumn("primary_key", sha2(concat_cols, 256))

def process_data():
    spark = create_spark_session()
    extract_zip_data()

    df = load_data_with_filename(spark)
    df = extract_file_date(df)
    df = extract_brand(df)
    df = add_storage_ranking(df)
    df = add_primary_key(df)

    df.write.mode("overwrite").option("header", True).csv("reports/final_result")
    spark.stop()

if __name__ == "__main__":
    process_data()
