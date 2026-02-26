export PYSPARK_SUBMIT_ARGS := --conf spark.sql.catalogImplementation=hive --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1 pyspark-shell

setup:
	uv venv

install:
	@uv lock
	@uv sync

format:
	@black src/

check:
	@ruff check src/

environment-up:
	docker compose up -d
	sleep 5
	docker compose ps

environment-down:
	docker compose down

environment-restart: environment-down environment-up

clean:
	rm -rf .venv uv.lock __pycache__

clean-spark:
	rm -rf data metastore_db spark-warehouse
	rm -rf /tmp/pipeline-storage

generate-batch:
	uv run python -m src.source.generate_batch

generate-stream:
	uv run python -m src.source.generate_stream

check-tables:
	uv run python -m src.check.check

pipeline-stream-python:
	spark-pipelines run --spec src/pipelines/stream/python/spark-pipelines.yaml

pipeline-stream-python:
	spark-pipelines run --spec src/pipelines/stream/python/spark-pipelines.yaml

pipeline-stream-python-full-refresh-all:
	spark-pipelines run --spec src/pipelines/stream/python/spark-pipelines.yaml --full-refresh-all

pipeline-stream-sql:
	spark-pipelines run --spec src/pipelines/stream/sql/spark-pipelines.yaml

pipeline-batch-python:
	spark-pipelines run --spec src/pipelines/batch/python/spark-pipelines.yaml

pipeline-batch-sql:
	spark-pipelines run --spec src/pipelines/batch/sql/spark-pipelines.yaml
