import pandas as pd


def rem_dubl():
    # Список имен файлов
    files = [
        "data_Chelyabinsk.csv", "data_EKB.csv", "data_Kazan.csv",
        "data_Krasnodar.csv", "data_Krasnoyyarsk.csv", "data_Moscow.csv",
        "data_NN.csv", "data_Novosibirsk.csv", "data_Omsk.csv",
        "data_Perm.csv", "data_Samara.csv", "data_SPB.csv",
        "data_Ufa.csv", "data_Volgograd.csv", "data_Voronezh.csv"
    ]

    for file_name in files:
        # Загрузка данных из файла
        data = pd.read_csv(file_name)

        # Удаление дубликатов
        data_cleaned = data.drop_duplicates()

        # Сохранение обработанных данных в тот же файл
        data_cleaned.to_csv(file_name, index=False)

        print(f"Обработан файл: {file_name}")