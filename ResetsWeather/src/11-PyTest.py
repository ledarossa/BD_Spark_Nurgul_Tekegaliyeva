from pyspark.sql import SparkSession

# Инициализация SparkSession без зависимости от Hadoop и winutils
spark = SparkSession.builder \
    .appName("RestaurantWeather") \
    .config("spark.driver.host", "localhost") \
    .getOrCreate()

# Путь к вашему файлу CSV (замените на актуальный путь)
file_path = "C:/Users/LEDA/PycharmProjects/ResetsWeather/Data/Enriched_Restaurants/enriched_restaurants_with_geohash.csv"

# Чтение данных из CSV
restaurants_df = spark.read.option("header", "true").csv(file_path)

# Показать схему данных
restaurants_df.printSchema()

# Показать первые несколько строк
restaurants_df.show()

# Например, чтобы проверить, сколько строк в DataFrame
print(f"Количество строк: {restaurants_df.count()}")
