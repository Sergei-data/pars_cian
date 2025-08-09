import requests
import json
import pandas as pd
import time
class Geocoder:

    base_url = "https://nominatim.openstreetmap.org/search"


    def fetch(self, address):

        try:
            params = {
                'q': address,
                'format': 'geojson'
            }

            res = requests.get(url=self.base_url, params=params)

            print(f"HTTP get request to URL: {res.url}, Status code {res.status_code}")

            if res.status_code == 200:
                return res
            else:
                return None
        except Exception as ex:
            return None



    def parse(self, res):
        try:
            data_coordinates = json.dumps(res["features"][0]["geometry"]["coordinates"], indent=2).replace('[', '').replace(']', '')
            return data_coordinates
        except Exception as ex:
            return None
    def run(self, city):
        df = pd.read_csv(city, header=None, names=['address'])
            # Получение данных из колонки
        column_data = df.iloc[:, 0].tolist()
        coordinates_array = []
        for list_address in range(1, len(column_data)):
            print(list_address - 1)
            res = self.fetch(column_data[list_address]).json()
            if res == None:
                time.sleep(10)
                coordinates = None
            else:
                coordinates = self.parse(res)
                coordinates_array.append(coordinates)
            time.sleep(1)
        df['coordinates'] = coordinates_array
        df.to_csv(city, index=False, encoding="UTF-8")

    def coord(self):
        list_city = [
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
        for city in list_city:
            self.run(city)


def run_geo():
    geocoder = Geocoder()
    geocoder.coord()