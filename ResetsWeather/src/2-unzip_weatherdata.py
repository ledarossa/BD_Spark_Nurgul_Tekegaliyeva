import zipfile
import os

def unzip_files(zip_folder, output_folder):
    zip_files = [f for f in os.listdir(zip_folder) if f.endswith('.zip')]
    for zip_file in zip_files:
        zip_path = os.path.join(zip_folder, zip_file)
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(output_folder)

unzip_files('C:/Users/LEDA/PycharmProjects/ResetsWeather/Data/weather_zips', 'C:/Users/LEDA/PycharmProjects/ResetsWeather/Data/weather_unzipped')

def remove_unnecessary_files(folder):
    for root, dirs, files in os.walk(folder):
        for file in files:
            if file.endswith('.crc') or file == 'SUCCESS':
                os.remove(os.path.join(root, file))

remove_unnecessary_files('Data/weather_unzipped')

def delete_crc_files(folder_path):
    # Проходим через все подпапки и файлы в указанной директории
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            # Если файл имеет расширение .crc
            if file.endswith('.crc'):
                file_path = os.path.join(root, file)
                os.remove(file_path)
                print(f"Удален файл: {file_path}")

# Укажите путь к корневой папке, откуда нужно удалить .crc файлы
delete_crc_files('C:/Users/LEDA/PycharmProjects/ResetsWeather/Data/weather_unzipped/weather')