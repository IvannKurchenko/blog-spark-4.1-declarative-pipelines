"""Synthetic data generator for IoT cold-chain warehouse monitoring demo.

Produces two Pandas DataFrames:
- sensors: ~10-row dimension table with sensor metadata.
- readings: ~10,000-row fact table with sensor measurements spanning 7 days.
"""

import uuid
from datetime import datetime, timedelta, timezone

import pandas as pd
from faker import Faker

fake = Faker()

WAREHOUSE_LOCATIONS = [
    "warehouse-A",
    "warehouse-B",
    "warehouse-C",
    "warehouse-D",
    "warehouse-E",
]

SENSOR_TYPES = ["temperature", "humidity"]

NUM_SENSORS = 10
START_TIME = datetime(2024, 1, 15, 0, 0, 0, tzinfo=timezone.utc)
DAYS = 7
INTERVAL_MINUTES = 10


def generate_sensors() -> pd.DataFrame:
    """Generate a sensors dimension table with id, location, and type."""
    rows = []
    for i in range(1, NUM_SENSORS + 1):
        sensor_type = fake.random_element(SENSOR_TYPES)
        rows.append(
            {
                "sensor_id": f"sensor-{i:03d}",
                "location": fake.random_element(WAREHOUSE_LOCATIONS),
                "sensor_type": sensor_type,
            }
        )
    return pd.DataFrame(rows)


def generate_readings(sensors: pd.DataFrame) -> pd.DataFrame:
    """Generate a readings fact table with one reading per sensor every ~10 minutes over 7 days."""
    total_intervals = (DAYS * 24 * 60) // INTERVAL_MINUTES
    rows = []

    for interval in range(total_intervals):
        ts = START_TIME + timedelta(minutes=interval * INTERVAL_MINUTES)
        ts_str = ts.strftime("%Y-%m-%dT%H:%M:%SZ")

        for _, sensor in sensors.iterrows():
            sid = sensor["sensor_id"]
            stype = sensor["sensor_type"]

            if stype == "temperature":
                value = round(fake.pyfloat(min_value=-5.0, max_value=10.0), 2)
                unit = "°C"
            else:
                value = round(fake.pyfloat(min_value=30.0, max_value=70.0), 2)
                unit = "%RH"

            rows.append(
                {
                    "reading_id": str(uuid.uuid4()),
                    "sensor_id": sid,
                    "timestamp": ts_str,
                    "value": value,
                    "unit": unit,
                }
            )

    return pd.DataFrame(rows)
