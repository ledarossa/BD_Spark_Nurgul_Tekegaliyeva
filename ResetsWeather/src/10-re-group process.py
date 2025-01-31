from pyspark.sql import SparkSession

# Инициализация Spark
spark = SparkSession.builder \
    .appName("RearrangeColumns") \
    .master("local[*]") \
    .config("spark.sql.warehouse.dir", "file:///C:/temp/spark-warehouse") \
    .getOrCreate()

# Путь к файлу
restaurants_file_path = "file:///C:/Users/LEDA/PycharmProjects/ResetsWeather/Data/Enriched_Restaurants/enriched_restaurants_with_geohash.csv"

# Чтение данных без явной схемы
restaurants_df = spark.read.csv(restaurants_file_path, header=True, inferSchema=True)

# Перегруппировка колонок в нужном порядке
restaurants_df_rearranged = restaurants_df.select(
    "id", "franchise_id", "franchise_name", "restaurant_franchise_id",
    "country", "city", "geohash", "lng", "lat"
)

# Запись результата с перезаписью и сохранением заголовков
restaurants_df_rearranged.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("file:///C:/Users/LEDA/PycharmProjects/ResetsWeather/Data/Enriched_Restaurants/rearranged_enriched_restaurants_with_geohash.csv")

# Закрытие Spark сессии
spark.stop()