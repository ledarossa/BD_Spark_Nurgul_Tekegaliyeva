import pandas as pd

# Путь к объединённому файлу
file_path = r"C:\Users\LEDA\PycharmProjects\ResetsWeather\Data\Combined\restaurants_weather_combined.csv"

# Загрузка данных
df = pd.read_csv(file_path)

# Удаление дубликатов
df_cleaned = df.drop_duplicates()

# Сохранение очищенного файла
df_cleaned.to_csv(file_path, index=False)

print(f"Файл очищен от дубликатов и сохранён по пути: {file_path}")
