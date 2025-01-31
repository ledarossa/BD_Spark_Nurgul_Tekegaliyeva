import os
import pandas as pd
import geohash2

# Путь к папке с CSV файлами
weather_folder_path = r"C:/Users/LEDA/PycharmProjects/ResetsWeather/Data/Weather_combined"

# Список всех CSV файлов в папке
csv_files = [f for f in os.listdir(weather_folder_path) if f.endswith('.csv')]

# Путь к выходному файлу
output_path = r"C:/Users/LEDA/PycharmProjects/ResetsWeather/Data/Weather_combined/weather_combined_with_geohash.csv"

# Создание выходного файла с заголовками
header_written = False

# Обработка данных по частям
for file in csv_files:
    file_path = os.path.join(weather_folder_path, file)
    weather_df = pd.read_csv(file_path)

    # Добавление геохэша
    weather_df['geohash'] = weather_df.apply(
        lambda row: geohash2.encode(row['lat'], row['lng'], precision=4) if pd.notnull(row['lat']) and pd.notnull(
            row['lng']) else None,
        axis=1
    )

    # Запись в файл с добавлением новых данных
    if not header_written:
        weather_df.to_csv(output_path, mode='w', header=True, index=False)
        header_written = True
    else:
        weather_df.to_csv(output_path, mode='a', header=False, index=False)

print(f"Объединенные данные с геохэшами сохранены в {output_path}")
