# Spark 4.1 Declarative Pipelines by Example

## Introduction

This repository contains companion code for the blog post "Spark 4 by example: Declarative pipelines".
It demonstrates how to build batch and streaming ETL pipelines using Spark 4.1 Declarative Pipelines,
with both Python and SQL implementations.

The demo models an IoT cold-chain warehouse monitoring scenario: sensors across multiple warehouse
locations produce temperature and humidity readings, which are ingested, transformed, and materialized
into fact and dimension tables.

## Prerequisites

The following tools must be installed before running the code:

- **Python 3.11**
- **Apache Spark 4.1**
- **uv** - Python package manager ([install guide](https://docs.astral.sh/uv/getting-started/installation/))
- **Docker** and **Docker Compose** - required for running Kafka

## How to

### Initial setup

Create a virtual environment and install dependencies:

```bash
make setup
make install
```

### Running batch pipelines

Run a batch pipeline using Python transformations:

```bash
make pipeline-batch-python
```

Or using SQL transformations:

```bash
make pipeline-batch-sql
```

These commands will automatically start Kafka, generate batch data as Parquet files, run the pipeline, and verify the
output tables.

### Running streaming pipelines

Run a streaming pipeline using Python transformations:

```bash
make pipeline-stream-python
```

Or using SQL transformations:

```bash
make pipeline-stream-sql
```


