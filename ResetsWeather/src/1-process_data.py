import pandas as pd
import requests
import geohash2

# Чтение CSV файлов
data1 = pd.read_csv('../Data/part-00000-c8acc470-919e-4ea9-b274-11488238c85e-c000.csv')
data2 = pd.read_csv('../Data/part-00001-c8acc470-919e-4ea9-b274-11488238c85e-c000.csv')
data3 = pd.read_csv('../Data/part-00002-c8acc470-919e-4ea9-b274-11488238c85e-c000.csv')
data4 = pd.read_csv('../Data/part-00003-c8acc470-919e-4ea9-b274-11488238c85e-c000.csv')
data5 = pd.read_csv('../Data/part-00004-c8acc470-919e-4ea9-b274-11488238c85e-c000.csv')

# Объединение всех данных в один DataFrame
data = pd.concat([data1, data2, data3, data4, data5], ignore_index=True)

# Проверка на пропущенные значения
print(data[['lat', 'lng']].isnull().sum())
# Функция для получения координат с использованием OpenCage Geocoding API
def get_lat_lon_from_api(address):
    api_key = '0c5cc49386d246758c630aad9b9ada94'
    url = f'https://api.opencagedata.com/geocode/v1/json?q={address}&key={api_key}'
    response = requests.get(url).json()
    if response['results']:
        lat = response['results'][0]['geometry']['lat']
        lon = response['results'][0]['geometry']['lng']
        return lat, lon
    return None, None

# Заполнение пропущенных значений
updated_count = 0
for index, row in data.iterrows():
    if pd.isnull(row['lat']) or pd.isnull(row['lng']):
        address = f"{row['city']}, {row ['country']}"
        lat, lon = get_lat_lon_from_api(address)
        if lat is not None and lon is not None:
            data.at[index, 'latitude'] = lat
            data.at[index, 'longitude'] = lon
            updated_count += 1

print(f"Number of rows updated: {updated_count}")

# Проверка, что пропущенные значения больше не присутствуют
print("After filling null values:")
print(data[['lat', 'lng']].isnull().sum())

# Оставляем только нужные столбцы
columns_needed = ['id', 'franchise_id', 'franchise_name', 'restaurant_franchise_id', 'country', 'city', 'lng',  'lat']
data = data[columns_needed]

# Сохранение измененных данных
data.to_csv('data/enriched_restaurants.csv', index=False)

# Функция для получения геохэша по координатам
def generate_geohash(lat, lon):
    # Генерация геохэша длиной 4 символа
    return geohash2.encode(lat, lon)[:4]

# Загрузка данных
data = pd.read_csv('../Data/enriched_restaurants.csv')

# Применяем функцию для генерации геохэша и добавляем в новый столбец
data['geohash'] = data.apply(lambda row: generate_geohash(row['lat'], row['lng']), axis=1)

# Проверяем результат
print(data[['geohash', 'lng', 'lat' ]].head())

# Сохраняем обновленные данные с геохэшами
data.to_csv('Data/enriched_restaurants_with_geohash.csv', index=False)