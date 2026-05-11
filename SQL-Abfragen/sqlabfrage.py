import sqlite3

DB_PATH = "database.db"


def show_temperature_stats(cursor):
    cursor.execute("""
        SELECT
            MAX(temperature) AS max_temp,
            MIN(temperature) AS min_temp,
            AVG(temperature) AS avg_temp
        FROM weather
        WHERE DATE(timestamp) = '2023-03-14';
    """)

    result = cursor.fetchone()

    print("Temperaturwerte am 2023-03-14:")
    print(f"Maximale Temperatur: {result[0]}")
    print(f"Minimale Temperatur: {result[1]}")
    print(f"Durchschnittstemperatur: {result[2]}")


def show_high_pm10_days(cursor):
    cursor.execute("""
        SELECT
            DATE(timestamp) AS datum,
            MAX(P1) AS max_pm10
        FROM air_quality
        GROUP BY DATE(timestamp)
        HAVING MAX(P1) > 50
        ORDER BY max_pm10 DESC;
    """)

    results = cursor.fetchall()

    print()
    print("Tage mit PM10-Wert über 50:")

    if not results:
        print("Keine Tage gefunden.")
        return

    for row in results:
        datum = row[0]
        max_pm10 = row[1]

        print(f"{datum}: {max_pm10}")


def main():
    connection = sqlite3.connect(DB_PATH)
    cursor = connection.cursor()

    try:
        show_temperature_stats(cursor)
        show_high_pm10_days(cursor)

    except Exception as error:
        print("Fehler beim Ausführen der SQL-Abfragen:")
        print(error)

    finally:
        connection.close()


if __name__ == "__main__":
    main()