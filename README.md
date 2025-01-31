# BD_Spark_Nurgul_Tekegaliyeva
 ResetsWeather (restaurant-geodata-processing)

## 📌 Task Description
This project is a Spark-based ETL job that processes restaurant and weather data, ensuring data accuracy and enrichment.

### ✅ **Objectives**
- Validate and correct missing latitude and longitude values using OpenCage Geocoding API.
- Generate a **4-character geohash** for each location.
- Perform a **left join** between restaurant and weather data using geohash.
- Ensure the job is **idempotent** (does not cause data duplication).
- Store the enriched data in **Parquet format** with partitioning (by country).

## 🛠️ **Tools and applied libraries**
- **Python** (PySpark, Pandas)
- **IDE** PyCharm
- **Notepad++**
- **OpenCage Geocoding API**
- **pandas as pd**
- **requests**
- **geohash2**
- **zipfile**
- **os**
  **pyspark.sql —> SparkSession**
  **pyspark.sql.function —> col**
- **Parquet format**
- **csv format**

## 📂 **Project Structure**
**src**
1-process_data.py
2-unzip_weatherdata.py
3-process_restaurant_weather.py
4-testPY2.py
5-weather_read.py
6-EnrichJDK.py
7-weather_geo.py
8-process_weather.py
9-clear_dups.py
10-re-group process.py
11-PyTest.py
12-left combine.py
13-Parqueting.py
14-check_result.py
15-check_random.py
**Resulting file**
📂 C:\Users\LEDA\PycharmProjects\ResetsWeather\Data\Combined
