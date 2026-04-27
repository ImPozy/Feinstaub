import sqlite3
import csv
from pathlib import Path

DB_PATH = "database.db"
DATA_FOLDER = "data"


def to_float(value):
    if value is None or value == "":
        return None
    return float(value)


def to_int(value):
    if value is None or value == "":
        return None
    return int(value)


def create_unique_indexes(cursor):
    cursor.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_weather_unique
        ON weather(sensor_id, timestamp)
    """)

    cursor.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_air_quality_unique
        ON air_quality(sensor_id, timestamp)
    """)


def import_weather(cursor, row):
    cursor.execute("""
        INSERT OR IGNORE INTO weather (
            sensor_id, sensor_type, location, lat, lon, timestamp,
            temperature, humidity
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        to_int(row["sensor_id"]),
        row["sensor_type"],
        to_int(row["location"]),
        to_float(row["lat"]),
        to_float(row["lon"]),
        row["timestamp"],
        to_float(row["temperature"]),
        to_float(row["humidity"])
    ))

    return cursor.rowcount


def import_air_quality(cursor, row):
    cursor.execute("""
        INSERT OR IGNORE INTO air_quality (
            sensor_id, sensor_type, location, lat, lon, timestamp,
            P1, durP1, ratioP1, P2, durP2, ratioP2
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        to_int(row["sensor_id"]),
        row["sensor_type"],
        to_int(row["location"]),
        to_float(row["lat"]),
        to_float(row["lon"]),
        row["timestamp"],
        to_float(row["P1"]),
        to_float(row["durP1"]),
        to_float(row["ratioP1"]),
        to_float(row["P2"]),
        to_float(row["durP2"]),
        to_float(row["ratioP2"])
    ))

    return cursor.rowcount


def import_csv_file(cursor, file_path):
    imported_rows = 0
    skipped_rows = 0

    with open(file_path, "r", encoding="utf-8", newline="") as file:
        reader = csv.DictReader(file, delimiter=";")

        for row in reader:
            sensor_type = row.get("sensor_type", "").upper()

            if sensor_type == "DHT22":
                inserted = import_weather(cursor, row)

            elif sensor_type == "SDS011":
                inserted = import_air_quality(cursor, row)

            else:
                print(f"Unbekannter sensor_type in Datei {file_path.name}: {sensor_type}")
                continue

            if inserted == 1:
                imported_rows += 1
            else:
                skipped_rows += 1

    return imported_rows, skipped_rows


def main():
    data_path = Path(DATA_FOLDER)

    if not data_path.exists():
        print(f"Ordner nicht gefunden: {DATA_FOLDER}")
        return

    csv_files = sorted(data_path.glob("*.csv"))

    if not csv_files:
        print("Keine CSV-Dateien gefunden.")
        return

    connection = sqlite3.connect(DB_PATH)
    cursor = connection.cursor()

    total_imported = 0
    total_skipped = 0

    try:
        create_unique_indexes(cursor)

        for csv_file in csv_files:
            imported, skipped = import_csv_file(cursor, csv_file)

            total_imported += imported
            total_skipped += skipped

            print(
                f"{csv_file.name}: "
                f"{imported} neu importiert, "
                f"{skipped} übersprungen"
            )

        connection.commit()

        print()
        print("Fertig.")
        print(f"Neu importiert: {total_imported}")
        print(f"Übersprungen: {total_skipped}")

    except Exception as error:
        connection.rollback()
        print("Fehler beim Import. Änderungen wurden zurückgesetzt.")
        print(error)

    finally:
        connection.close()


if __name__ == "__main__":
    main()

# Test