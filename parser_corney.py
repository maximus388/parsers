# Парсер данных по новостройкам с сайта застройщика ЖК Корней в Тюмени
# https://kornei.ru/#about


import time
import datetime

import pandas as pd

from urllib.request import urlopen
from lxml import etree

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import WebDriverException
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities

import pygsheets
import progressbar


chrome_options = Options()
# chrome_options.add_experimental_option("detach", True)             # Оставлять браузер открытым

URL = 'https://kornei.ru/kvartiry/'
ACTUAL_DATE = datetime.datetime.now().strftime('%Y-%m-%d')
ZHK = 'Корней'

def import_data(URL):
    global ACTUAL_DATE
    caps = DesiredCapabilities().CHROME
    caps["pageLoadStrategy"] = "none"
    browser = webdriver.Chrome(service = Service(ChromeDriverManager().install()), desired_capabilities = caps)
    browser.get(URL)
    time.sleep(7)
    flats_overall_num = int(browser.find_element(By.XPATH, "//span[@class='parametric-results__label']").text)
    extend = browser.find_element(By.XPATH, "//button[@class='button button_theme_content-width button_theme_light button_theme_stroke']")
    for i in range(flats_overall_num // 6):
        try:
#             time.sleep(time_to_extend)                   # из-за этого браузер закрывается раньше нужного
            browser.execute_script("arguments[0].click();", extend)
        except:
            pass
    print('Получение данных...')
    links = [link.get_attribute("href") for link in browser.find_elements(By.XPATH, "//*[@href]")]
    links = [i for i in links if 'https://kornei.ru/kvartiry/flat/' in i]
    if flats_overall_num == len(links): # потом поставить "равно"
        df = []
        with progressbar.ProgressBar(max_value = len(links)) as bar:
            for link in links:
                response = urlopen(link)
                htmlparser = etree.HTMLParser()
                tree = etree.parse(response, htmlparser)
                name = tree.xpath("""//h1[@class='flat__title']/text()""")
                flat_num = int(tree.xpath("""//h1[@class='flat__title']/text()""")[0].split(' ')[-1])
                deadline = tree.xpath("""//div[@class='flat__deadline']/text()""")
                area_total = tree.xpath("""//ul[@class='flat__params']//b/text()""")[0].replace('\xa0м', '').replace('.', ',')
                floor_num = int(tree.xpath("""//ul[@class='flat__params']//b/text()""")[-1])
                floor_cnt = int(tree.xpath("""//ul[@class='flat__params']//li/text()""")[-1].replace(u'\xa0', u' ').split(' ')[-2])
                price = tree.xpath("""//p[@class='flat__prices-main']/text()""")[0].strip().replace(u' ', u'').replace(u'\xa0', u' ').split(' ')[0]
                df_link = pd.DataFrame({'actual_date': ACTUAL_DATE, 'zhk': ZHK, 'deadline': deadline, 'entrance_num': 1, 'flat_num': flat_num, 
                                        'name': name, 'area_total': area_total, 'floor_num': floor_num, 'floor_cnt': floor_cnt, 'price': price, 'link': link})
                df.append(df_link)
                bar.update(links.index(link))
        df = pd.concat(df).reset_index(drop = True)
        df['name'] = df['name'].replace({'Студия': 1, 
                                         'Однокомнатная квартира': 1, 
                                         'Двухкомнатная квартира': 2, 
                                         'Трехкомнатная квартира': 3, 
                                         'Четырехкомнатная квартира': 4,
                                         'Пятикомнатная квартира': 5}, regex = True)
        df = df.rename(columns={"name": "rooms_cnt"})
    return df

def export_to_google(df):
    print('Экспорт данных в гугл-таблицу...')
    try:
        gc = pygsheets.authorize(service_file='Z:\\Аналитический отдел\\Python обучение\\test service account.json')
    except FileNotFoundError:
        gc = pygsheets.authorize(service_file='C:\\Users\\ws-tmn-an-15\\Desktop\\Харайкин М.А\\Python документы\\python-automation-script-jupyter-notebook-266007-21fda3e2971a.json')
    sh = gc.open_by_key('1MEekXLn0Snza2P7TnLf8G4YsSVB9o4fxhkDB-CVD-bQ') # https://docs.google.com/spreadsheets/d/1MEekXLn0Snza2P7TnLf8G4YsSVB9o4fxhkDB-CVD-bQ/edit#gid=158442293
    wks = sh.worksheet_by_title('Выгрузка - Корней')
    wks.clear(start = '', end = '')
    wks.set_dataframe(df, 'a1', extend = True, nan = '')

    
def main():
    df = import_data(URL)
    df[f'Дата обновления: {ACTUAL_DATE}\nОбновлено: Харайкин М.А.'] = ''
    export_to_google(df)
    
if __name__ == '__main__':
    main()
    print('Скрипт выполнен!')
