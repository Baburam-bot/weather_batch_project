import traceback
import sys
from pyspark.sql.utils import AnalysisException
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql import functions as F
import os

# GCP project and paths
PROJECT_ID = "weather-batch-project"
BUCKET_NAME = "weather-pipeline-bucket"
BLOB_PATH = "extraction/weather_data.json"
GCS_PATH = f"gs://{BUCKET_NAME}/{BLOB_PATH}"
BQ_DATASET = "weather_dataset"
BQ_TABLE = "weather_table"

# BigQuery schema (optional but recommended)
BQ_SCHEMA = [
    "latitude:FLOAT",
    "longitude:FLOAT",
    "timezone:STRING",
    "timezone_abbreviation:STRING",
    "utc_offset_seconds:INT64",
    "elevation:FLOAT",
    "time:STRING",
    "temperature_2m:FLOAT",
    "wind_speed_10m:FLOAT",
    "wind_direction_10m:FLOAT",
    "relative_humidity_2m:FLOAT",
    "precipitation:FLOAT",
    "weathercode:INT64",
    "pressure_msl:FLOAT",
    "cloudcover:FLOAT",
    "visibility:FLOAT",
    "dewpoint_2m:FLOAT"
]

def main():
    try:
        # Set Google credentials
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/baburammarandi/Documents/weather_batch_project/weather-batch-project-21efd4465537.json"

        # Configure Spark
        conf = (
            SparkConf()
            .setAppName("Weather Nested Load")
            .set(
                "spark.jars",
                "/Users/baburammarandi/jars/gcs-connector-hadoop3-2.2.5-shaded.jar,"
                "/Users/baburammarandi/jars/spark-bigquery-with-dependencies_2.12-0.31.0.jar",
            )
            .set("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
            .set("spark.hadoop.google.cloud.auth.service.account.enable", "true")
            .set("spark.hadoop.fs.gs.auth.type", "SERVICE_ACCOUNT_JSON_KEYFILE")
            .set("spark.hadoop.fs.gs.auth.service.account.json.keyfile",
                 "/Users/baburammarandi/Documents/weather_batch_project/weather-batch-project-21efd4465537.json")
        )

        # Initialize Spark session
        spark = SparkSession.builder.config(conf=conf).getOrCreate()
        spark.sparkContext.setLogLevel("INFO")
        print("Spark session started.")

        # 1) Read JSON
        try:
            print(f"Reading JSON data from {GCS_PATH} ...")
            raw_df = spark.read.option("multiline", "true").json(GCS_PATH)
            print(f"Read {raw_df.count()} rows")
            raw_df.printSchema()
        except Exception as e:
            print("Error reading JSON:")
            traceback.print_exc(file=sys.stdout)
            raise e

        # 2) Transform: Align hourly arrays using posexplode_outer
        try:
            hourly = raw_df.select(
                "*",
                F.posexplode_outer("hourly.time").alias("idx", "time")
            )

            final_df = hourly.select(
                "latitude",
                "longitude",
                "timezone",
                "timezone_abbreviation",
                "utc_offset_seconds",
                "elevation",
                "time",
                F.col("hourly.temperature_2m").getItem(F.col("idx")).alias("temperature_2m"),
                F.col("hourly.wind_speed_10m").getItem(F.col("idx")).alias("wind_speed_10m"),
                F.col("hourly.wind_direction_10m").getItem(F.col("idx")).alias("wind_direction_10m"),
                F.col("hourly.relative_humidity_2m").getItem(F.col("idx")).alias("relative_humidity_2m"),
                F.col("hourly.precipitation").getItem(F.col("idx")).alias("precipitation"),
                F.col("hourly.weathercode").getItem(F.col("idx")).alias("weathercode"),
                F.col("hourly.pressure_msl").getItem(F.col("idx")).alias("pressure_msl"),
                F.col("hourly.cloudcover").getItem(F.col("idx")).alias("cloudcover"),
                F.col("hourly.visibility").getItem(F.col("idx")).alias("visibility"),
                F.col("hourly.dewpoint_2m").getItem(F.col("idx")).alias("dewpoint_2m"),
            )

            print("Preview transformed data:")
            final_df.show(10, truncate=False)
            print(f"Total transformed rows: {final_df.count()}")
        except Exception as e:
            print("Error during transform:")
            traceback.print_exc(file=sys.stdout)
            raise e

        # 3) Write to BigQuery (overwrite)
        try:
            print(f"Writing to BigQuery {PROJECT_ID}:{BQ_DATASET}.{BQ_TABLE} ...")
            final_df.write \
                .format("bigquery") \
                .option("table", f"{PROJECT_ID}:{BQ_DATASET}.{BQ_TABLE}") \
                .option("writeMethod", "direct") \
                .option("project", PROJECT_ID) \
                .mode("overwrite") \
                .save()
            print("Data successfully written to BigQuery.")
        except Exception as e:
            print("Error writing to BigQuery:")
            traceback.print_exc(file=sys.stdout)
            raise e

        spark.stop()
        print("Spark session stopped.")
    
    except Exception as e:
        print("Job failed:", e)
        traceback.print_exc(file=sys.stdout)
        raise

if __name__ == "__main__":
    main()
