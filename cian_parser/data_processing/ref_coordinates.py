import pandas as pd

def ref_coor():
    files = [
        "data_Chelyabinsk.csv", "data_EKB.csv", "data_Kazan.csv", "data_Krasnodar.csv",
        "data_Krasnoyarsk.csv", "data_Moscow.csv", "data_NN.csv", "data_Novosibirsk.csv",
        "data_Omsk.csv", "data_Perm.csv", "data_Samara.csv", "data_SPB.csv",
        "data_Ufa.csv", "data_Volgograd.csv", "data_Voronezh.csv"
    ]

    for file in files:
        data = pd.read_csv(file)

        # Очистка и разделение координат
        data[['longitude', 'latitude']] = data['coordinates'].str.extract(r'([\d\.\-]+),\s*([\d\.\-]+)')

        # создание доп столбцов
        data['longitude'] = pd.to_numeric(data['longitude'], errors='coerce')
        data['latitude'] = pd.to_numeric(data['latitude'], errors='coerce')

        # Удаление столбца coordinates
        data.drop(columns=['coordinates'], inplace=True)


        data.to_csv(file, index=False)