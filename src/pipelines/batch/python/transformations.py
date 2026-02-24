import os
from pathlib import Path

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame

spark = SparkSession.builder.getOrCreate()


@dp.materialized_view(
    name="dim_sensors",
    comment="Sensors dimensional table",
    table_properties={"type": "materialized_view"}
)
def dim_sensors_mv() -> DataFrame:
    data_frame = spark.read.parquet("./data/sensors.parquet")
    return data_frame


@dp.materialized_view(
    name="fact_readings",
    comment="Fact readings table",
    table_properties={"type": "materialized_view"}
)
def fact_readings_mv() -> DataFrame:
    data_frame = spark.read.parquet("./data/readings.parquet")
    return data_frame


@dp.materialized_view(
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
