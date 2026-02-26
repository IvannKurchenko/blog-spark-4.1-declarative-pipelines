import os
from pathlib import Path

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

spark = SparkSession.builder.getOrCreate()


@dp.materialized_view(
    name="dim_sensors",
    comment="Sensors dimensional table",
)
def dim_sensors_mv() -> DataFrame:
    data_frame = spark.read.parquet("./data/sensors.parquet")
    return data_frame


@dp.table(
    name="fact_readings",
    comment="Fact table with sensors readings",
)
def fact_readings() -> DataFrame:
    schema = StructType([
        StructField("reading_id", StringType()),
        StructField("sensor_id", StringType()),
        StructField("timestamp", StringType()),
        StructField("value", DoubleType()),
        StructField("unit", StringType()),
    ])

    readings_df = (
        spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "readings")
        .option("startingOffsets", "earliest")
        .load()
        .select(F.from_json(F.col("value").cast("string"), schema).alias("data"))
        .select("data.*")
    )
    return readings_df

@dp.table(
    name="fact_warehouse_temperature",
    comment="Fact table of temperature statistics per location",
)
def fact_warehouse_temperature_mv() -> DataFrame:
    data_frame = (
        spark
        .table("fact_readings")
        .join(spark.table("dim_sensors"), "sensor_id")
        .where(F.col("sensor_type") == "temperature")
        .groupby("location")
        .agg(F.max("value").alias("max_temperature"))
    )
    return data_frame
