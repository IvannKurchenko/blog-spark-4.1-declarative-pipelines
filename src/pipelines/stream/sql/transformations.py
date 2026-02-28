from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

spark = SparkSession.builder.getOrCreate()

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
