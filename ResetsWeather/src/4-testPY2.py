from pyspark.sql import SparkSession
import os

# Инициализация Spark
spark = SparkSession.builder \
    .appName("ReadWeatherData") \
    .master("local[*]") \
    .getOrCreate()

# Укажите путь к папке с файлами Parquet
weather_folder_path = "file:///C:/Users/LEDA/PycharmProjects/ResetsWeather/Data/weather_unzipped"

# Проверка на существование папки перед чтением
local_path = weather_folder_path.replace("file:///", "")  # Преобразуем путь в локальный формат для os
if not os.path.exists(local_path):
    print(f"Папка {local_path} не существует. Проверьте путь!")
else:
    try:
        # Чтение всех файлов Parquet из указанной папки
        weather_df = spark.read.parquet(weather_folder_path)

        # Вывод первых строк
        print("Первые 5 строк из данных о погоде:")
        weather_df.show(15)

        # Вывод схемы данных
        print("Схема данных:")
        weather_df.printSchema()

        # Подсчет количества строк
        print(f"Количество строк: {weather_df.count()}")

        # Проверка содержимого, если в данных ожидаются определенные колонки
        expected_columns = ["lng", "lat", "avg_tmpr_c", "wthr_date"]  # Замените на ваши столбцы
        actual_columns = weather_df.columns
        missing_columns = [col for col in expected_columns if col not in actual_columns]

        if missing_columns:
            print(f"Внимание! Отсутствуют следующие ожидаемые колонки: {missing_columns}")
        else:
            print("Все ожидаемые колонки присутствуют.")
    except Exception as e:
        print(f"Произошла ошибка при чтении данных: {e}")

# Остановка Spark
spark.stop()
