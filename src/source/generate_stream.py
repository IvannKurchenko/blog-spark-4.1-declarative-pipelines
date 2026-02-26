"""Generate stream data for the IoT cold-chain warehouse monitoring demo.

Writes sensors as Parquet (if not exists) and publishes each reading as a JSON message to Kafka.
"""

import json
import logging
from pathlib import Path

from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

from src.source.data import generate_readings, generate_sensors

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC = "readings"

DATA_DIR = Path(".") / "data"
SENSORS_FILE = DATA_DIR / "sensors.parquet"


def write_sensors_if_missing(sensors):
    DATA_DIR.mkdir(exist_ok=True)
    if SENSORS_FILE.exists():
        log.info("Skipping sensors — %s already exists", SENSORS_FILE)
        return
    sensors.to_parquet(SENSORS_FILE, index=False)
    log.info("Wrote sensors (%d rows) to %s", sensors.shape[0], SENSORS_FILE)


def ensure_topic_exists():
    admin = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP)
    try:
        # Short retention so Kafka drops old messages quickly — useful for demoing pipeline refresh behaviour.
        admin.create_topics([NewTopic(
            name=KAFKA_TOPIC,
            num_partitions=1,
            replication_factor=1,
            topic_configs={"retention.ms": "10000"},
        )])
        log.info("Created topic '%s'", KAFKA_TOPIC)
    except TopicAlreadyExistsError:
        log.info("Topic '%s' already exists", KAFKA_TOPIC)
    finally:
        admin.close()


def publish_readings(readings):
    ensure_topic_exists()
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    count = 0
    for _, row in readings.iterrows():
        producer.send(KAFKA_TOPIC, value=row.to_dict())
        count += 1
        if count % 1000 == 0:
            log.info("Published %d / %d readings", count, len(readings))

    producer.flush()
    producer.close()
    log.info("Published all %d readings to topic '%s'", count, KAFKA_TOPIC)


def main():
    sensors = generate_sensors()
    readings = generate_readings(sensors)

    write_sensors_if_missing(sensors)
    publish_readings(readings)


if __name__ == "__main__":
    main()
