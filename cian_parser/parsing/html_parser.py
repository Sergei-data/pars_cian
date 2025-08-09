import cian_parser.data_about_city
import requests
from bs4 import BeautifulSoup
from fake_headers import Headers
import time
import csv
import json
import logging

data_city = ''

headers = Headers(
    browser="chrome",
    os="win",
    headers=True
)

def count_home(min_price, max_price, url):
    response = requests.get(url, headers=headers.generate())
    soup = BeautifulSoup(response.text, "lxml")
    data = soup.find("div", class_="_93444fe79c--serp--bTAO_ _93444fe79c--serp--light--moDYM")
    count_home = data.find("h5", class_="_93444fe79c--color_black_100--kPHhJ _93444fe79c--lineHeight_20px--tUURJ _93444fe79c--fontWeight_bold--ePDnv _93444fe79c--fontSize_14px--TCfeJ _93444fe79c--display_block--pDAEx _93444fe79c--text--g9xAG _93444fe79c--text_letterSpacing__normal--xbqP6").text
    num = int(''.join(filter(lambda i: i.isdigit(), count_home))) / 25
    
    if num == 0:
        return 0
    return int(num) + 1

def get_url():

    with open("list_city.json", "r", encoding="utf-8") as f:
        list_city = json.load(f)
    
    with open("list_data_files.json", "r", encoding="utf-8") as f:
        list_data_files = json.load(f)

    for city in range(15):
        global data_city
        min_price = 1
        max_price = 5_000_000
        url = list_city[city]
        data_city = list_data_files[city]

        for j in range(50):
            try:
                min_price, max_price += 5_000_000
                count_page = count_home(min_price, max_price, url)

                for num_site_page in range(count_page + 1):
                    response = requests.get(url, headers=headers.generate())
                    soup = BeautifulSoup(response.text, "lxml")
                    data = soup.find("div", class_="_93444fe79c--serp--bTAO_ _93444fe79c--serp--light--moDYM").find("div", class_="_93444fe79c--wrapper--W0WqH")
                    card_url = data.find_all("article", class_="_93444fe79c--container--Povoi _93444fe79c--cont--OzgVc")
                    time_add = data.find_all("span", class_="_93444fe79c--color_gray60_100--MlpSF _93444fe79c--lineHeight_20px--tUURJ _93444fe79c--fontWeight_normal--P9Ylg _93444fe79c--fontSize_14px--TCfeJ _93444fe79c--display_inline--bMJq9 _93444fe79c--text--g9xAG _93444fe79c--text_letterSpacing__normal--xbqP6")
                    week_day = 0
                    ind = 0
                    while week_day < len(time_add):
                        list_forbidden_words = ["неделю", "недели", "месяца", "месяц", "полгода", "год"]
                        for word in list_forbidden_words:
                            if word in time_add[week_day].text:
                                break
                        else:
                            link = card_url[ind].find("a", class_="_93444fe79c--media--9P6wN").get("href")
                            yield link
                        ind += 1
                        week_day += 2
            except Exception as ex:
                logging.error(f"Ошибка при получении URL: {ex}")
                continue


def set_type_of_data_block(data_block):
    for i in range(len(data_block)):
            match data_block[i].text:
                case "Тип жилья":
                    type_of_home = data_block[i+1].text
                case "Общая площадь":
                    square_home = float(data_block[i+1].text.replace(",", ".").strip())
                case "Жилая площадь":
                    live_square = float(data_block[i+1].text.replace(",", ".").strip())
                case "Площадь кухни":
                    kitchen_square = float(data_block[i+1].text.replace(",", ".").strip())
                case "Высота потолков":
                    height_ceiling = data_block[i+1].text
                    height_ceiling = height_ceiling[:-2]
                    height_ceiling = float(height_ceiling.replace(",", ".").strip())
                case "Санузел":
                    bathroom = data_block[i+1].text
                    bathroom = int(bathroom[:1])
                case "Балкон/лоджия":
                    balcony = data_block[i+1].text
                    balcony = int(balcony[:1])
                case "Ремонт":
                    repair = data_block[i+1].text
                case "Год постройки":
                    year_house = int(data_block[i+1].text.strip())
                case "Мусоропровод":
                    garbage_chute = data_block[i+1].text
                case "Тип дома":
                    type_home = data_block[i+1].text
                case "Тип перекрытий":
                    type_of_overlap = data_block[i+1].text
                case "Подъезды":
                    entrances = int(data_block[i+1].text.strip())
                case "Отопление":
                    heating = data_block[i+1].text
                case "Аварийность":
                    accident_rate = data_block[i+1].text
                case "Газоснабжение":
                    gas_supply = data_block[i+1].text
                case "Отделка":
                    finishing = data_block[i+1].text
                case "Количество лифтов":
                    number_of_elevators = data_block[i+1].text
                    number_of_elevators = int(number_of_elevators[:1].strip())



def array():
    with open(data_city, 'a', encoding="utf8", newline='') as file:
                writer = csv.writer(file)
                writer.writerow(
                (
                    name, price, price_for_metr, floor_object, total_floors, type_of_home, square_home, live_square,
                    kitchen_square, height_ceiling, bathroom, balcony, repair, year_house, garbage_chute, type_home,
                    type_of_overlap, entrances, heating, accident_rate, gas_supply, finishing, number_of_elevators,
                    address, get_link
                )
    )

    for get_link in get_url():
        time.sleep(1)

        try:
            response_object = requests.get(get_link, headers=headers.generate())
            soup = BeautifulSoup(response_object.text, "lxml")

            data_object = soup.find("div", class_="a10a3f92e9--page--OYngf")
            name = data_object.find("div", class_="a10a3f92e9--container--pWxZo")\
                              .find("h1", class_="a10a3f92e9--title--vlZwT").text
            index_for_name = name.find(',')
            name = name[:index_for_name]


            price = data_object.find("div", class_="a10a3f92e9--amount--ON6i1").find("span").text
            price = int(''.join(filter(lambda i: i.isdigit(), price)))

            price_for_metr = data_object.find("div", class_="a10a3f92e9--item--iWTsg").text
            price_for_metr = int(price_for_metr[12: -4].replace(" ", ""))

            floor_from_block = data_object.find_all("div", class_="a10a3f92e9--item--Jp5Qv")
            floor = 'Null'

            for i in floor_from_block:
                if i.text[:4] == "Этаж":
                    floor = i.text[4:]

            index_for_floor = floor.find(' ')
            floor_object = int(floor[:index_for_floor])
            index_for_floors = floor.rfind(' ')
            total_floors = int(floor[index_for_floors + 1:])
            data_block = data_object.find("div", class_="a10a3f92e9--container--rGqFe").find_all("span")

            # о квартире и дом
            type_of_home = '' # Тип жилья
            square_home = '' # Общая площадь
            live_square = '' # Жилая площадь
            kitchen_square = '' # Площадь кухни
            height_ceiling = '' # Высота потолков
            bathroom = '' # Санузел
            balcony = '' # Балкон/лоджия
            repair = '' # Ремонт
            year_house = ''  # Год постройки
            garbage_chute = ''  # Мусоропровод
            type_home = ''  # Тип дома
            type_of_overlap = ''  # Тип перекрытий
            entrances = ''  # Подъезды
            heating = ''  # Отопление
            accident_rate = ''  # Аварийность
            gas_supply = ''  # Газоснабжение
            finishing = '' # Отделка
            number_of_elevators = '' # Количество лифтов

            set_type_of_data_block(data_block)

            address = data_object.find("div", class_="a10a3f92e9--header-information--w7fS9").find("span").get("content")

        except Exception as ex:
            logging.error(f"Ошибка при обработке ссылки {get_link}: {ex}")
            continue

