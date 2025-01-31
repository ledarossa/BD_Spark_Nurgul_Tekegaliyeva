from pyspark.sql import SparkSession

# Инициализация Spark
spark = SparkSession.builder \
    .appName("CSV_to_Parquet_Conversion") \
    .master("local[*]") \
    .getOrCreate()

# Путь к исходному файлу CSV и директории для сохранения Parquet
csv_file_path = "file:///C:/Users/LEDA/PycharmProjects/ResetsWeather/Data/Combined/restaurants_weather_combined.csv"
parquet_file_path = "file:///C:/Users/LEDA/PycharmProjects/ResetsWeather/Data/Combined/restaurants_weather_combined.parquet"

# Чтение данных из CSV файла
df = spark.read.csv(csv_file_path, header=True, inferSchema=True)

# Проверка количества строк и колонок в исходных данных
print(f"Количество строк в исходном CSV: {df.count()}")
print(f"Количество колонок в исходном CSV: {len(df.columns)}")

# Сохранение данных в формате Parquet с разделением по колонке 'country'
df.write \
    .partitionBy('country') \
    .mode("overwrite") \
    .parquet(parquet_file_path)

# Проверка целостности данных после преобразования
# Загрузка данных из Parquet файла
df_parquet = spark.read.parquet(parquet_file_path)

# Проверка количества строк и колонок в Parquet файле
print(f"Количество строк в Parquet: {df_parquet.count()}")
print(f"Количество колонок в Parquet: {len(df_parquet.columns)}")

# Проверка того, что данные не были повреждены
if df.count() == df_parquet.count() and len(df.columns) == len(df_parquet.columns):
    print("Целостность данных сохранена!")
else:
    print("Данные были повреждены при преобразовании!")

# Закрытие Spark сессии
spark.stop()
