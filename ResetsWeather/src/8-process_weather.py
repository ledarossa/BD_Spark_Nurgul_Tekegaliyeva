import pandas as pd

# Функция для загрузки и обработки данных о погоде
def process_weather_data():
    # Загрузка данных о погоде
    weather_data = pd.read_csv('data/weather.csv')

    # Например, удалим ненужные столбцы или заполним пропуски
    weather_data = weather_data.dropna(subset=['geohash'])  # Удаляем строки без геохэша

    # Возвращаем обработанные данные
    return weather_data