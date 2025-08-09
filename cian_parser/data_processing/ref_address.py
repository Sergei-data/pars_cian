import pandas as pd


def reformat_address(address, city):
    # Удаляем типы улиц и лишние пробелы из адреса
    parts = address.split(',')
    if len(parts) >= 5:
        # Удаление  'ул.', 'пер.', 'мкр.', и удаление лишних пробелов
        street = ' '.join(part for part in parts[-2].strip().split(' ') if part not in ['ул.', 'пер.', 'мкр.'])
        building = parts[-1].strip()
        return f"Россия, {city}, {street}, {building}"
    return address

def process_city_addresses(file_path, city_map):
    # Определяем название города из имени файла и переводим его на русский
    city_key = file_path.split('_')[1].split('.')[0]
    city = city_map.get(city_key, "Unknown")


    data = pd.read_csv(file_path)

    # Применяем функцию реформатирования адреса, используя название города из словаря
    data['reformatted_address'] = data['address'].apply(lambda x: reformat_address(x, city))

    # Сохраняем
    output_file = file_path.replace('.csv', '_processed.csv')
    data.to_csv(output_file, index=False)



# Словарь перевода названий городов
city_map = {
    "Chelyabinsk": "Челябинск",
    "EKB": "Екатеринбург",
    "Kazan": "Казань",
    "Krasnodar": "Краснодар",
    "Krasnoyarsk": "Красноярск",
    "Moscow": "Москва",
    "NN": "Нижний Новгород",
    "Novosibirsk": "Новосибирск",
    "Omsk": "Омск",
    "Perm": "Пермь",
    "Samara": "Самара",
    "SPB": "Санкт-Петербург",
    "Ufa": "Уфа",
    "Volgograd": "Волгоград",
    "Voronezh": "Воронеж"
}


files = [
    "data_Chelyabinsk.csv",
    "data_EKB.csv",
    "data_Kazan.csv",
    "data_Krasnodar.csv",
    "data_Krasnoyarsk.csv",
    "data_Moscow.csv",
    "data_NN.csv",
    "data_Novosibirsk.csv",
    "data_Omsk.csv",
    "data_Perm.csv",
    "data_Samara.csv",
    "data_SPB.csv",
    "data_Ufa.csv",
    "data_Volgograd.csv",
    "data_Voronezh.csv"
]

def rename_address():
    for file_name in files:
        process_city_addresses(file_name, city_map)
