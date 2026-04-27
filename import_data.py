import sqlite3
import csv
from pathlib import Path

DB_PATH = "database.db"
DATA_FOLDER = "data"


def to_float(value):
    if value == "" or value is None:
        return None
    return float(value)


def to_int(value):
    if value == "" or value is None:
        return None
    return int(value)


def import_weather(cursor, row):
    cursor.execute("""
        INSERT INTO weather (
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


def import_air_quality(cursor, row):
    cursor.execute("""
        INSERT INTO air_quality (
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


def import_csv_file(cursor, file_path):
    with open(file_path, "r", encoding="utf-8", newline="") as file:
        reader = csv.DictReader(file, delimiter=";")

        imported_rows = 0

        for row in reader:
            sensor_type = row.get("sensor_type", "").upper()

            if sensor_type == "DHT22":
                import_weather(cursor, row)
                imported_rows += 1

            elif sensor_type == "SDS011":
                import_air_quality(cursor, row)
                imported_rows += 1

            else:
                print(f"Unbekannter sensor_type in Datei {file_path.name}: {sensor_type}")

        return imported_rows


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

    total_rows = 0

    try:
        for csv_file in csv_files:
            rows = import_csv_file(cursor, csv_file)
            total_rows += rows
            print(f"{csv_file.name}: {rows} Zeilen importiert")

        connection.commit()
        print(f"\nFertig. Insgesamt importiert: {total_rows} Zeilen")

    except Exception as error:
        connection.rollback()
        print("Fehler beim Import. Änderungen wurden zurückgesetzt.")
        print(error)

    finally:
        connection.close()


if __name__ == "__main__":
    main()