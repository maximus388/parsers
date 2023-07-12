# Парсер данных по квартирам в ЖК Машаров в городе Тюмени с сайта застройщика


import pandas as pd
import requests
import progressbar
import datetime
import pygsheets

from bs4 import BeautifulSoup

ACTUAL_DATE = datetime.datetime.now().strftime('%Y-%m-%d')

TRIES = 10

df_flats = pd.DataFrame()
print("Получение списка квартир...")
with progressbar.ProgressBar(max_value = len(range(TRIES))) as bar:
    for i in range(TRIES):
        res = requests.get(f"https://backend.masharov.aerokod.ru/api/apartments?page={i+1}")
        json_data = res.json()
        df0 = pd.json_normalize(json_data['data'])
        if len(df0) > 0:
            df_flats = pd.concat([df_flats, df0])
            bar.update(range(TRIES).index(i))
        else:
            break
flats_id_list = df_flats['id'].to_list()

DF = pd.DataFrame()
print()
print("Получение данных по квартирам...")
with progressbar.ProgressBar(max_value = len(flats_id_list)) as bar:
    for flat in flats_id_list:
        page = requests.get(f'https://masharov-kvartal.ru/apartments/{flat}')
        soup = BeautifulSoup(page.text, "html.parser")
        try:
            soup.find('div', class_='b-apartments-detail__status').text
        except AttributeError:
            res = requests.get(f"https://backend.masharov.aerokod.ru/api/apartments/{flat}")
            json_flat = res.json()
            df_flat = pd.json_normalize(json_flat)
            DF = pd.concat([DF, df_flat])
            bar.update(flats_id_list.index(flat))
        else:
            bar.update(flats_id_list.index(flat))
            pass


DF = DF.rename(columns={'data.number': 'flat_num'
                        , 'data.count_rooms': 'rooms_cnt'
                        , 'data.house.name': 'gp_name'
                        , 'data.section': 'entrance_num'
                        , 'data.area_total': 'area_total'
                        , 'data.price': 'price'
                        , 'data.floor.number': 'floor_num'}).reset_index(drop = True)

DF = DF[['flat_num', 'rooms_cnt', 'gp_name', 'entrance_num', 'area_total', 'price', 'floor_num']]
DF[['flat_num', 'entrance_num', 'price', 'floor_num']] = DF[['flat_num', 'entrance_num', 'price', 'floor_num']].astype('Int64')
DF['area_total'] = [str(val).replace('.', ',') for val in DF['area_total']]
DF.insert(0, 'zhk_name', "Авторский Квартал «Машаров»")
DF.insert(0, 'actual_date', datetime.datetime.now().strftime('%Y-%m-%d'))


print('Экспорт данных в гугл-таблицу...')
gc = pygsheets.authorize(service_file='<service_file>')
sh = gc.open_by_key('<google_sheets_id>')
wks = sh.worksheet_by_title('Выгрузка')
wks.clear(start = '', end = '')
wks.set_dataframe(DF, 'a1', extend = True, nan = '')
