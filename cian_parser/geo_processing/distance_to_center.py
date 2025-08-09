import pandas as pd
import numpy as np


def haversine(lon1, lat1, lon2, lat2):
    R = 6371
    lon1, lat1, lon2, lat2 = map(np.radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = np.sin(dlat / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2) ** 2
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))
    distance = R * c
    return distance

def update_datasets_with_distances():


    city_centers = {
        "data_Chelyabinsk.csv": (61.4008, 55.1603),
        "data_EKB.csv": (60.6057, 56.8389),
        "data_Kazan.csv": (49.1082, 55.7963),
        "data_Krasnodar.csv": (38.9753, 45.0355),
        "data_Krasnoyarsk.csv": (92.8932, 56.0153),
        "data_Moscow.csv": (37.6173, 55.7558),
        "data_NN.csv": (43.9361, 56.2965),
        "data_Novosibirsk.csv": (82.9357, 55.0084),
        "data_Omsk.csv": (73.3686, 54.9924),
        "data_Perm.csv": (56.3167, 58.0000),
        "data_Samara.csv": (50.2212, 53.2415),
        "data_SPB.csv": (30.3351, 59.9343),
        "data_Ufa.csv": (55.9721, 54.7388),
        "data_Volgograd.csv": (44.5133, 48.7080),
        "data_Voronezh.csv": (39.2003, 51.6608)
    }

    # Перебор файлов и расчет расстояний
    for file_name, (center_lon, center_lat) in city_centers.items():

        data = pd.read_csv(file_name)

        # Расчет расстояний
        data['distance_to_center_km'] = haversine(center_lon, center_lat, data['longitude'], data['latitude'])


        data.to_csv(file_name, index=False)