from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("EnrichData") \
    .master("local[*]") \
    .config("spark.sql.warehouse.dir", "file:///C:/temp/spark-warehouse") \
    .config("spark.executor.memory", "8g") \
    .config("spark.driver.memory", "8g") \
    .config("spark.memory.fraction", "0.8") \
    .getOrCreate()
