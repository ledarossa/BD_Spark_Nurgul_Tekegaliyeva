import pandas as pd

# Путь к файлам
weather_file_path = r"C:\Users\LEDA\PycharmProjects\ResetsWeather\Data\Weather_combined\weather_combined_with_geohash.csv"
restaurants_file_path = r"C:\Users\LEDA\PycharmProjects\ResetsWeather\Data\Enriched_Restaurants\enriched_restaurants_with_geohash.csv"

# Чтение данных о погоде
weather_df = pd.read_csv(weather_file_path)

# Размер чанка
chunk_size = 50  # Можно настроить в зависимости от объема данных и доступной памяти

# Генерация чанков из файла ресторанов
chunks = pd.read_csv(restaurants_file_path, chunksize=chunk_size)

# Путь для сохранения результата
output_path = r"C:\Users\LEDA\PycharmProjects\ResetsWeather\Data\Combined\restaurants_weather_combined.csv"

# Обработка чанков
combined_df_list = []
for chunk in chunks:
    # Выполнение операции merge (left join) по геохэшу
    combined_chunk = pd.merge(chunk, weather_df, on='geohash', how='left')

    # Добавление результата в список для последующего объединения
    combined_df_list.append(combined_chunk)

# Объединение всех чанков в один DataFrame
final_combined_df = pd.concat(combined_df_list, ignore_index=True)

# Сохранение объединённых данных в итоговый файл
final_combined_df.to_csv(output_path, index=False)

print(f"Объединённый файл сохранён по пути: {output_path}")
