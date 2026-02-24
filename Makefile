export PROJECT_ROOT := $(shell pwd)
export PYSPARK_SUBMIT_ARGS := --conf spark.sql.catalogImplementation=hive pyspark-shell

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

environment-down:
	docker compose down

clean:
	rm -rf .venv uv.lock __pycache__

clean-spark:
	rm -rf data metastore_db spark-warehouse

generate-batch: environment-down environment-up
	uv run python -m src.source.generate_batch

generate-stream: environment-down environment-up
	uv run python -m src.source.generate_stream

check-tables:
	uv run python -m src.check.check

pipeline-stream-python: generate-stream
	spark-pipelines run --spec src/pipelines/stream/python/spark-pipelines.yaml
	$(MAKE) check-tables

pipeline-stream-sql: generate-stream
	spark-pipelines run --spec src/pipelines/stream/sql/spark-pipelines.yaml
	$(MAKE) check-tables

pipeline-batch-python: generate-batch
	spark-pipelines run --spec src/pipelines/batch/python/spark-pipelines.yaml
	$(MAKE) check-tables

pipeline-batch-python-full-refresh-all: generate-batch
	spark-pipelines run --spec src/pipelines/batch/python/spark-pipelines.yaml --full-refresh-all
	$(MAKE) check-tables

pipeline-batch-sql:
	spark-pipelines run --spec src/pipelines/batch/sql/spark-pipelines.yaml
	$(MAKE) check-tables

help:
	@echo "Usage: make [target]"
	@echo ""
	@echo "Setup:"
	@echo "  setup                    Create virtual environment"
	@echo "  install                  Install dependencies"
	@echo "  clean                    Clean environment"
	@echo "  clean-spark              Remove Spark data directories"
	@echo ""
	@echo "Code quality:"
	@echo "  format                   Format code"
	@echo "  check                    Run linter checks"
	@echo ""
	@echo "Environment:"
	@echo "  environment-up           Start Kafka via Docker Compose"
	@echo "  environment-down         Stop Kafka via Docker Compose"
	@echo ""
	@echo "Data generation:"
	@echo "  generate-batch           Generate batch Parquet data files"
	@echo "  generate-stream          Restart Kafka and publish readings to it"
	@echo ""
	@echo "Pipelines:"
	@echo "  pipeline-stream-python   Run stream pipeline (Python)"
	@echo "  pipeline-stream-sql      Run stream pipeline (SQL)"
	@echo "  pipeline-batch-python    Run batch pipeline (Python)"
	@echo "  pipeline-batch-sql       Run batch pipeline (SQL)"

.PHONY: setup install clean clean-spark format check environment-up environment-down generate-batch generate-stream check-tables pipeline-stream-python pipeline-stream-sql pipeline-batch-python pipeline-batch-sql help
