## Spark 4 by example: Declarative pipelines
![img_2.png](img_2.png)

### Introduction
In this series of blog posts we will walk though the new features and capabilities introduced to Spark 4 major and all
current minor versions showcasing each by example you can run by easily yourself.

This post focuses on of the major freshly features Declarative Pipeline.

### Overview
Spark Declarative Pipelines (SDP) have been introduced in Spark 4.1 version. SDP removes a need to organize 
a Direct Acyclic Graph of transformations by doing this for you. All is needed to be done is just a register components
that should work together. SDP figures out dependencies, build the execution plan and run it.  
The key components of the declarative pipelines are:
- `Flows` - declares a process of reading data, transforming and writing to sink.
- `Dataset` - declares an object. Currently supported `Temporary Views`, `Materialized Views` and `Streaming Tables`.
Please note that regular tables and views are not supported. 
- `Pipelines` - declares a set of flows and dataset into a single executable execution unit.

SDP can be written using PySpark or SQL. Java and Scala SDKs are not supported at the moment of this writing.
SDP also supports both batch and streaming pipelines, that will be shown later.

### Demo example
For the sake of the demonstration of SDP capabilities lets consider the following small hypothetical example of an
IoT system that consist of temperature and humidity sensors that produce a number of readings. 
Sensors data is available at `./data/sensors.parquet` and has the following schema:

| Column        | Type   | Description                        |                                                                                                               
|---------------|--------|------------------------------------|                                                                                                             
| `sensor_id`   | string | Formatted sensor identifier        |                                                                                                               
| `location`    | string | Warehouse location                 |                                                                                                               
| `sensor_type` | string | Either `temperature` or `humidity` |

Readings are present either in `./data/readings.parquet` or published to  local Kafka to `readings` topic as json.

| Column       | Type   | Description                                           |
|--------------|--------|-------------------------------------------------------|
| `reading_id` | string | UUID v4                                               |
| `sensor_id`  | string | FK to `sensors.sensor_id`                             |
| `timestamp`  | string | ISO 8601 UTC timestamp (10-min intervals over 7 days) |
| `value`      | float  | Measurement value (temp: -5..10, humidity: 30..70)    |
| `unit`       | string | `°C` for temperature, `%RH` for humidity              |

The data for both data sources is synthetically generated. 

The goal would be to write a pipeline to ingest data from either of the sources and calculate average temperature per
location. 

### Batch pipeline using Python 
For batch pipelines SDP relies on temporary views and materialized views for this case. Any data source that you would 
like to work with SDP needs to be lifted to either of two objects. Python SDK provides a pretty easy and convenient
way to declare the objects by using decorators from `pyspark.pipelines` package. 
Let's write `transformations.py` that will contain all the pipeline objects.
```python
from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

@dp.materialized_view(
    name="dim_sensors",
    comment="Sensors dimensional table",
)
def dim_sensors_mv() -> DataFrame:
    data_frame = spark.read.parquet("./data/sensors.parquet")
    return data_frame


@dp.materialized_view(
    name="fact_readings",
    comment="Fact table with sensors readings ",
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
```
After we have this declaration in place, we need to create the SDP YAML configuration file `spark-pipeline.yaml`:
```yaml
# The name of pipeline
name: declarative_pipeline
# Source code to be used for the pipeline.
libraries:
  - glob:
      # The one is described above.
      include: ./transformations.py
# Path to the streaming checkpointing storage.
storage: file:///tmp/pipeline-storage
```
Although any streaming capabilities are not used so far, we still need to declare `storage` configuration for structured
streaming checkpointing.

Now we have two main components we can run the pipeline using `spark-pipelines` command. For instance:
```shell
spark-pipelines run --spec src/pipelines/batch/python/spark-pipelines.yaml
```

After the run we can explore local catalog and find newly created tables. Example:
```text
dim_sensors | Total rows: 10
+----------+-----------+-----------+
|sensor_id |location   |sensor_type|
+----------+-----------+-----------+
|sensor-001|warehouse-E|temperature|
|sensor-002|warehouse-D|temperature|
+----------+-----------+-----------+

Table: fact_readings | Total rows: 10080
+------------------------------------+----------+--------------------+-----+----+
|reading_id                          |sensor_id |timestamp           |value|unit|
+------------------------------------+----------+--------------------+-----+----+
|35cc3448-58de-4a37-8f77-6ee1761d950d|sensor-001|2024-01-15T00:00:00Z|-1.17|°C  |
|553bb0a6-b8c3-435a-b227-7098d19eaf6c|sensor-002|2024-01-15T00:00:00Z|8.83 |°C  |
+------------------------------------+----------+--------------------+-----+----+

Table: fact_warehouse_temperature | Total rows: 5
+-----------+---------------+
|location   |max_temperature|
+-----------+---------------+
|warehouse-C|10.0           |
|warehouse-E|9.98           |
+-----------+---------------+
```

SDP recomputes and overwrites materialized views on each run, so if we regenerate upstream data and re-run the 
`spark-pipelines` again we will observe completely new numbers.

Although if this is this is not desired, `spark-pipelines run` has `--refresh` configuration allowing to specify
datasets to recompute only.

### Batch pipeline using SQL
The same SDP pipeline can be implemented using plain SQL, like in the following example:
```sql
CREATE MATERIALIZED VIEW dim_sensors
COMMENT 'Sensors dimensional table'
AS SELECT * FROM parquet.`./data/sensors.parquet`;

CREATE MATERIALIZED VIEW fact_readings
COMMENT 'Fact table with sensors readings '
AS SELECT * FROM parquet.`./data/readings.parquet`;

CREATE TABLE fact_warehouse_temperature
COMMENT 'Fact table of temperature statistics per location'
AS SELECT
    s.location,
    MAX(r.value) AS max_temperature
FROM fact_readings r
JOIN dim_sensors s ON r.sensor_id = s.sensor_id
WHERE s.sensor_type = 'temperature'
GROUP BY s.location;
```
Configuration would look almost identical:
```yaml
name: declarative_pipeline
libraries:
  - glob:
      include: ./transformations.sql
storage: file:///tmp/pipeline-storage
```

However, in `include` option can also be specified a file pattern like `folder/**` for convenience.
After having this in place same `spark-pipelines run` can be used to run the pipeline.

### Stream pipeline using Python
Now we have a look at the SDP stream capabilities. SDP can declare streaming table among dataset. Unlike materialized views,
streaming tables updated with each run using `append` mode, instead of `overwrite`.

For the sake of demonstration lets consider an example of locally running Kafka with `readings` topic which contains
JSON's sensors readings of the same content as we've seen in `readings.parquet` before. The complete solution would look 
the following:
```python
from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
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
```

Pay attention to the following changes in comparison to batch update:
- `fact_readings` - are declared now with `@dp.table` to create streaming tables.
- `@dp.table` - creates **streaming** but not a regular catalog table.
-  `fact_warehouse_temperature` - remains as materialized view, because it contains aggregations, that can be written in append mode.   

Now we can run this pipeline using same `spark-pipelines.yaml` and `spark-pipelines` command. The first run would give
similar result as in batch use case - we will have same number of rows in the table as in the topic (~10K).
Although, if messages have been removed from the Kafka's topic and new have been published after, second pipeline run
would result into twice bigger `fact_readings` table size because messages have been appended. So resulting table would
look similar to:
```text
fact_readings | Total rows: 20160
+------------------------------------+----------+--------------------+-----+----+
|reading_id                          |sensor_id |timestamp           |value|unit|
+------------------------------------+----------+--------------------+-----+----+
|4692625f-d613-4b8d-a058-44517208a449|sensor-001|2024-01-15T00:00:00Z|61.59|%RH |
|8e590181-e60a-43ad-9111-18527dd8f2af|sensor-002|2024-01-15T00:00:00Z|66.71|%RH |
+------------------------------------+----------+--------------------+-----+----+
```

`spark-pipelines run` command has a special option `--full-refresh-all` that works only for streaming dataset, which 
essentially recomputes the streaming table from the scratch. In our case, that would mean truncating the table and writing
all the messages from the Kafka topic.

### Stream pipeline using SQL
Although it is possible to do declare streaming table in Spark SQL using `CREATE STREAMING TABLE` statement,
It possible to declare steaming tables from the other tables, but at sources like Kafka topic.

### Pitfalls
Although SDP has also a number of this to be aware about, such that each dataset declaration should cause
no side effects (e.g. call `write` operation).
See more in the [Important Considerations](https://spark.apache.org/docs/latest/declarative-pipelines-programming-guide.html#important-considerations)


#### References:
- [Spark Declarative Pipelines Programming Guide](https://spark.apache.org/docs/latest/declarative-pipelines-programming-guide.html)
- [Introducing Apache Spark® 4.1](https://www.databricks.com/blog/introducing-apache-sparkr-41)
- [Lakeflow Spark Declarative Pipelines concepts](https://docs.databricks.com/aws/en/ldp/concepts)
- [PySpark Declarative Pipelines SDK](https://spark.apache.org/docs/latest/api/python/reference/pyspark.pipelines.html)
- [A First Look at Declarative Pipelines in Apache Spark 4.0](https://medium.com/@willgirten/a-first-look-at-declarative-pipelines-in-apache-spark-4-0-751144e33278)
- [SPIP: Declarative Pipelines](https://issues.apache.org/jira/browse/SPARK-51727)
- [SPARK-48117: Spark Materialized Views: Improve Query Performance and Data Management](https://issues.apache.org/jira/browse/SPARK-48117)
- [Spark Declarative Pipelines (SDP) Explained in Under 20 Minutes](https://www.youtube.com/watch?v=WNPYEZ7SMSM)
- [Spark Scala Docs](https://api-docs.databricks.com/scala/spark/latest/org/apache/spark/index.html)
- [Spark Java Docs](https://spark.apache.org/docs/latest/api/java/index.html)