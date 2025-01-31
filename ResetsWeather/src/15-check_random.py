from pyspark.sql import SparkSession

# Инициализация Spark
spark = SparkSession.builder \
    .appName("View Random Parquet Data") \
    .master("local[*]") \
    .getOrCreate()

# Путь к директории с Parquet файлами
parquet_file_path = "file:///C:/Users/LEDA/PycharmProjects/ResetsWeather/Data/Combined/restaurants_weather_combined.parquet"

# Загрузка данных из Parquet файла
df_parquet = spark.read.parquet(parquet_file_path)

# Выборка случайных строк (приблизительно 40 строк)
random_sample_df = df_parquet.sample(fraction=0.01, seed=42).limit(40)  # fraction можно подстраивать, чтобы получить около 40 строк

# Показ случайных 40 строк
print("Случайные 40 строк из Parquet файла:")
random_sample_df.show(40, truncate=False)

# Закрытие Spark сессии
spark.stop()
