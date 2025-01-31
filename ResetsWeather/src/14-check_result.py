from pyspark.sql import SparkSession

# Инициализация Spark
spark = SparkSession.builder \
    .appName("View Parquet Data") \
    .master("local[*]") \
    .getOrCreate()

# Путь к директории с Parquet файлами
parquet_file_path = "file:///C:/Users/LEDA/PycharmProjects/ResetsWeather/Data/Combined/restaurants_weather_combined.parquet"

# Загрузка данных из Parquet файла
df_parquet = spark.read.parquet(parquet_file_path)

# Просмотр любых 40 строк
print("Первые 40 строк из Parquet файла:")
df_parquet.show(40, truncate=False)

# Закрытие Spark сессии
spark.stop()
