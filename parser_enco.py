# Парсер данных по новостройкам с сайта застройщика Энко
# https://enco72.ru/zhilye-kompleksy/



from urllib.request import urlopen
from lxml import etree
import pandas as pd
import datetime
import progressbar


ACTUAL_DATE = datetime.datetime.now().strftime('%Y-%m-%d')

# Выгрузка списка ЖК застройщика
def import_zhk_list():
    global ACTUAL_DATE 

    print("importing list of newhouses...")
    URL = 'https://enco72.ru/zhilye-kompleksy/'
    response = urlopen(URL)
    htmlparser = etree.HTMLParser()
    tree = etree.parse(response, htmlparser)

    zhk_name = tree.xpath("""//h2[@class='title']/text()""") # name of newhouse
    zhk_link = tree.xpath("""//article[@class='item']//a/@href""") # newhouse url
    zhk_link = ['https://enco72.ru' + i for i in zhk_link]

    df_zhk = {'actual_date': ACTUAL_DATE, 'zhk_name': zhk_name, 'zhk_link': zhk_link}
    df = pd.DataFrame(df_zhk)
    return df

# выгрузка кол-ва квартир
def import_flats_data(df):
    
    print("importing layouts...")
    with progressbar.ProgressBar(max_value = len(df)) as bar:
        df_layouts = []
        for zhk_link in df['zhk_link']:
            response = urlopen(zhk_link)
            htmlparser = etree.HTMLParser()
            tree = etree.parse(response, htmlparser)
            layout_link = tree.xpath("""//div[@class='card']/@data-href""")
            layout_link = ['https://enco72.ru' + layout for layout in layout_link]
            df_layouts.append(pd.DataFrame({'zhk_link': zhk_link, 'layout_link': layout_link}))
            bar.update(list(df['zhk_link']).index(zhk_link))
    df_layouts = pd.concat(df_layouts)

    df = df.merge(df_layouts, left_on = 'zhk_link', right_on = 'zhk_link', how = 'right')

    print("importing flats...")
    with progressbar.ProgressBar(max_value = len(df)) as bar:
        df_flats = []
        for layout in df['layout_link']:
            response = urlopen(layout)
            htmlparser = etree.HTMLParser()
            tree = etree.parse(response, htmlparser)
            flat_link = ['https://enco72.ru' + href for href in list(set(tree.xpath("""//a[@class='flat_link c']/@href""")))]
            df_flats.append(pd.DataFrame({'layout_link': layout, 'flat_link': flat_link}))
            bar.update(list(df['layout_link']).index(layout))
    df_flats = pd.concat(df_flats)

    df = df.merge(df_flats, left_on = 'layout_link', right_on = 'layout_link', how = 'right')
    df = df.drop_duplicates(subset = ['flat_link']).reset_index(drop = True)

    return df

# сбор информации по выгруженным квартирам    
def import_flats_param(df):
    print("importing flats info...")

    with progressbar.ProgressBar(max_value = len(df)) as bar:
        df_flats_info = []
        for flat in df['flat_link']:    
            response = urlopen(flat)
            htmlparser = etree.HTMLParser()
            tree = etree.parse(response, htmlparser)
            flat_num = tree.xpath("""//div[@class='flat-number']/text()""")[0].split('№')[-1].strip()
            rooms_cnt = tree.xpath("""//div[@class='flat-square']/text()""")[0].strip()
            area_total = tree.xpath("""//div[@class='flat-square']//span/text()""")[0].split(' ')[0].strip().replace('.', ',')
            price = tree.xpath("""//div[@class='flat-price js-flat-price']/text()""")[0].replace(' ', '').strip()[:-1]
            deadline = tree.xpath("""//div[@class='flat-info__value']/text()""")[1].strip()
            gp = tree.xpath("""//div[@class='flat-info__value']/text()""")[2].strip().split(', ')[0]
            try:
                entrance = tree.xpath("""//div[@class='flat-info__value']/text()""")[2].strip().split(', ')[1].split('Секция ')[1].replace('Cекция ', '')
            except IndexError:
                entrance = tree.xpath("""//div[@class='flat-info__value']/text()""")[2].strip().split(', ')[1].split('Секция ')[0].replace('Cекция ', '')
            floor_num = tree.xpath("""//div[@class='flat-info__value']/text()""")[3].split("/")[0].strip()
            df_flats_info.append(pd.DataFrame({'flat_link': flat, 'gp': gp, 'deadline': deadline, 'entrance': entrance, 'floor_num': floor_num, 
                                               'rooms_cnt': rooms_cnt, 'flat_num': flat_num, 'area_total': area_total, 'price': price}, index = [0]))
            bar.update(list(df['flat_link']).index(flat))
    df_flats_info = pd.concat(df_flats_info).reset_index(drop = True)
    df_flats_info['rooms_cnt'] = df_flats_info['rooms_cnt'].replace({'Студия': 1, 
                                                                     'Однокомнатная': 1, 
                                                                     'Двухкомнатная': 2, 
                                                                     'Трехкомнатная': 3, 
                                                                     'Четырехкомнатная': 4,
                                                                     'Пятикомнатная': 5}, regex = True)
    
    df = df.merge(df_flats_info, left_on = 'flat_link', right_on = 'flat_link', how = 'left')
    return df
  
def main():
    DF = import_zhk_list()
    DF = import_flats_data(DF)
    DF = import_flats_param(DF)
    print('Done!')

if __name__ == '__main__':
    main()
