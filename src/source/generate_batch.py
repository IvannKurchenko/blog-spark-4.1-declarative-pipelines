"""Generate batch data files for the IoT cold-chain warehouse monitoring demo.

Writes sensors and readings DataFrames as Parquet files into {PWD}/data/.
"""

import logging
import os
from pathlib import Path

from src.source.data import generate_readings, generate_sensors

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

DATA_DIR = Path(os.environ["PROJECT_ROOT"]) / "data"

SENSORS_FILE = DATA_DIR / "sensors.parquet"
READINGS_FILE = DATA_DIR / "readings.parquet"


def write(df, path: Path, name: str):
    if path.exists():
        path.unlink()
        log.info("Removed existing %s at %s", name, path)
    df.to_parquet(path, index=False)
    log.info("Wrote %s (%d rows) to %s", name, df.shape[0], path)


def main():
    DATA_DIR.mkdir(exist_ok=True)

    sensors = generate_sensors()
    readings = generate_readings(sensors)

    write(sensors, SENSORS_FILE, "sensors")
    write(readings, READINGS_FILE, "readings")


if __name__ == "__main__":
    main()
