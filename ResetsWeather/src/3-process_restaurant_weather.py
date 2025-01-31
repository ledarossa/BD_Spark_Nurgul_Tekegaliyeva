from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os

# Инициализация Spark с локальным режимом работы
spark = SparkSession.builder \
    .appName("EnrichData") \
    .master("local[*]") \
    .config("spark.sql.warehouse.dir", "file:///C:/temp/spark-warehouse") \
    .getOrCreate()

# Пути к файлам
restaurants_file_path = "file:///C:/Users/LEDA/PycharmProjects/ResetsWeather/Data/Enriched_Restaurants/enriched_restaurants_with_geohash.csv"
weather_folder_path = "file:///C:/Users/LEDA/PycharmProjects/ResetsWeather/Data/weather_unzipped"
intermediate_csv_path = "file:///C:/Users/LEDA/PycharmProjects/ResetsWeather/Data/weather_combined.csv"
output_parquet_path = "file:///C:/Users/LEDA/PycharmProjects/ResetsWeather/Data/Output"

# Чтение данных о ресторанах с дополнительными опциями для CSV
restaurants_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(restaurants_file_path)

# Чтение данных о погоде из директории
weather_df = spark.read.parquet(weather_folder_path)

# Преобразуем колонки lat и lng в формат double для совместимости
restaurants_df = restaurants_df.withColumn("lat", col("lat").cast("double")) \
    .withColumn("lng", col("lng").cast("double"))

weather_df = weather_df.withColumn("lat", col("lat").cast("double")) \
    .withColumn("lng", col("lng").cast("double"))

# Сохраняем данные о погоде в формат CSV
weather_df.write \
    .option("header", "true") \
    .csv(intermediate_csv_path)

# Чтение обратно данных о погоде из CSV (чтобы объединить их с ресторанами)
weather_csv_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(intermediate_csv_path)

# Выполняем left join по колонкам lat и lng
joined_df = restaurants_df.join(weather_csv_df, on=["lat", "lng"], how="left")

# Сохраняем результат в формате Parquet на локальной файловой системе
joined_df.write.mode("overwrite").parquet(output_parquet_path)

# Закрытие Spark сессии
spark.stop()