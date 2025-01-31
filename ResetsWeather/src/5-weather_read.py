from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Инициализация Spark с локальным режимом работы
spark = SparkSession.builder \
    .appName("EnrichData") \
    .master("local[*]") \
    .config("spark.sql.warehouse.dir", "file:///C:/temp/spark-warehouse") \
    .getOrCreate()

# Пути к файлам
weather_folder_path = "file:///C:/Users/LEDA/PycharmProjects/ResetsWeather/Data/weather_unzipped"
intermediate_csv_path = "file:///C:/Users/LEDA/PycharmProjects/ResetsWeather/Data/weather_combined.csv"

# Чтение данных о погоде из директории
weather_df = spark.read.parquet(weather_folder_path)

# Преобразуем колонки lat и lng в формат double для совместимости
weather_df = weather_df.withColumn("lat", col("lat").cast("double")) \
    .withColumn("lng", col("lng").cast("double"))

# Сохраняем данные о погоде в формат CSV (с перезаписью файла, если он уже существует)
weather_df.write \
    .option("header", "true") \
    .mode("overwrite") \
    .csv(intermediate_csv_path)

# Закрытие Spark сессии
spark.stop()