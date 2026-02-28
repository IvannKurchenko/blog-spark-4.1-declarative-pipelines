"""List all pipeline tables and show 2 rows per each."""

import logging
from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


def main():
    spark = SparkSession.builder.appName("Spark Check Tables").enableHiveSupport().getOrCreate()

    tables = [row.name for row in spark.catalog.listTables()]

    if not tables:
        log.info("No tables found in catalog")
        return

    for table in sorted(tables):
        df = spark.table(table)
        log.info("Table: %s | Total rows: %d", table, df.count())
        df.show(2, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
