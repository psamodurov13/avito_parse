# -*- coding: utf-8 -*-
import json
import os.path
import ssl
import requests as rq
import time
import datetime
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.poolmanager import PoolManager
from requests.packages.urllib3.util import ssl_
from bs4 import BeautifulSoup as bs
from loguru import logger
from fake_headers import Headers
import random as rd

# proxies = {
#     'https': 'http://--.---.---.---:8000'
#     'https': 'http://--.---.---.---:8000'
# }

logger.add('debug.log', format='{time} {level} {message}', level='INFO', rotation='10 KB', compression='zip')
CIPHERS = """ECDHE-RSA-AES256-GCM-SHA384:ECDHE-ECDSA-AES256-GCM-SHA384:ECDHE-RSA-AES256-SHA384:ECDHE-ECDSA-AES256-SHA384:ECDHE-RSA-AES128-GCM-SHA256:ECDHE-RSA-AES128-SHA256:AES256-SHA"""


class TlsAdapter(HTTPAdapter):
    def __init__(self, ssl_options=0, **kwargs):
        self.ssl_options = ssl_options
        super(TlsAdapter, self).__init__(**kwargs)

    def init_poolmanager(self, *pool_args, **pool_kwargs):
        ctx = ssl_.create_urllib3_context(ciphers=CIPHERS, cert_reqs=ssl.CERT_REQUIRED, options=self.ssl_options)
        self.poolmanager = PoolManager(*pool_args, ssl_context=ctx, **pool_kwargs)


def replacer(text):
    replace_dict = {'\xa0': ' '}
    for i in replace_dict:
        if i in text:
            text = text.replace(i, replace_dict[i])
        return text


@logger.catch
def start_parse():
    s = rq.Session()
    adapter = TlsAdapter(ssl.OP_NO_TLSv1 | ssl.OP_NO_TLSv1_1)
    s.mount("https://", adapter)
    # cookie из браузера
    cookie = ''
    # генерируем рандомные headers
    header = Headers(
        browser="chrome",  # Generate only Chrome UA
        # os="linux",  # Generate ony ___ platform
        headers=True  # generate misc headers
    )
    headers = [header.generate() for i in range(10)]
    for i in headers:
        i['Cookie'] = cookie

    # Прописываем интересующие регионы
    locs = ['maykop', 'ufa', 'ulan-ude', 'mahachkala', 'magas', 'nalchik', 'elista', 'petrozavodsk', 'syktyvkar',
            'simferopol', 'yoshkar-ola', 'saransk', 'yakutsk', 'vladikavkaz', 'kazan', 'kyzyl', 'izhevsk', 'abakan',
            'groznyy', 'cheboksary', 'barnaul', 'chita', 'petropavlovsk-kamchatskiy', 'krasnodar', 'krasnoyarsk',
            'perm', 'vladivostok', 'stavropol', 'habarovsk', 'amurskaya_oblast_blagoveschensk', 'arhangelsk',
            'astrahan', 'belgorod', 'bryansk', 'vladimir', 'volgograd', 'vologda', 'voronezh', 'ivanovo', 'irkutsk',
            'kaliningrad', 'kaluga', 'kemerovo', 'kirovskaya_oblast_kirov', 'kostroma', 'kurgan', 'kursk', 'lipetsk',
            'magadan', 'moskovskaya_oblast_krasnogorsk', 'nizhniy_novgorod', 'velikiy_novgorod', 'novosibirsk',
            'omsk', 'orenburg', 'orel', 'penza', 'pskov', 'rostov-na-donu', 'ryazan', 'samara', 'saratov',
            'yuzhno-sahalinsk', 'ekaterinburg', 'smolensk', 'tambov', 'tver', 'tomsk', 'tula', 'tyumen', 'ulyanovsk',
            'chelyabinsk', 'yaroslavl', 'moskva', 'sankt-peterburg', 'sevastopol', 'birobidzhan', 'naryan-mar',
            'hanty-mansiysk', 'anadyr', 'salehard']

    # Прописываем интересующие категории
    categories = {
        # '25': {'url': 'doma_dachi_kottedzhi', 'param': '[202]=1064'},
        '24': {'url': 'kvartiry', 'param': '[201]=1059'}
    }
    # Прописываем интересующие поля
    firebase = ['itemPrice', 'area', 'area_kitchen', 'area_live', 'commission',
                'floor', 'floors_count', 'house_type', 'nazvanie_obekta_nedvizhimosti', 'ofitsialnyy_zastroyshchik',
                'otdelka', 'rooms', 'status', 'type', 'categorySlug', 'distance_to_city', 'house_area', 'material_sten',
                'site_area', 'god_postroiki']
    params = ['Количество комнат', 'Статус', 'Балкон или лоджия', 'Тип комнат', 'Высота потолков, м', 'Санузел', 'Окна',
              'Отделка', 'Способ продажи', 'Коммуникации', 'Ремонт', 'Парковка', 'Мебель', 'Техника', 'Инфраструктура',
              'Терраса или веранда']
    # Переменные для сбора сводной информации
    count_except_city = 0
    skip = []
    count_no_ok = 0
    count_except_orders = 0
    count_except_pages = 0
    firebase_new = set()
    params_new = set()
    not_add = 0

    # Записываем прогресс в файл
    with open('progress.txt', 'w') as file:
        file.write(f'Запущено {str(datetime.datetime.now())}\n')

    # Запускаем проход по регионам
    for loc in locs:
        orders = {}
        city_rus = ''
        # Запускаем проход по категориям
        try:
            for cat in categories:
                r = s.request('GET',
                              f'https://www.avito.ru/{loc}/{categories[cat]["url"]}/prodam-ASgBAgICAUSSA8YQ?p=1&presentationType=serp',
                              headers=headers[rd.randint(0, 9)], timeout=60)
                # Достаем информацию о городе и количество страниц
                soup = bs(r.text, features='html.parser')
                loc_index = r.text.find('locationId=') + 11
                location_id = r.text[loc_index:loc_index + 6]
                city_rus = soup.select_one('.breadcrumbs-linkWrapper-jZP0j:first-child a').text
                pages = soup.select_one('.pagination-root-Ntd_O .pagination-item-JJq_j:nth-last-child(2)').text
                print(f'{"-"*40}\nГород: {str(city_rus)}\nСтраниц всего: {pages}\n')
                # Записываем информацию в progress.txt
                with open('progress.txt', 'a') as file:
                    file.write(f'{"-"*40}\nГород: {str(city_rus)}\nСтраниц всего: {pages}\n')
                time.sleep(2)
                min_price = 0
                # Запускаем проход по страницам категории
                flag = True
                while flag:
                    page = 1
                    curent_min_price = min_price
                    # После прохода 100 страниц обновляем значение curent_min_price и проходим по новым страницам
                    flag2 = True
                    while flag2:
                        try:
                            r_j = s.request('GET', f'https://m.avito.ru/api/11/items?key=af0deccbgcgidddjgnvljitntccdduijhdinfgjgfjir&params{categories[cat]["param"]}&categoryId={cat}&locationId={location_id}&page={page}&sort=priceAsc&priceMin={curent_min_price}&lastStamp=1660769220&display=list&limit=50',
                                            headers=headers[rd.randint(0, 9)], timeout=60)
                            r_json = json.loads(replacer(r_j.text))
                            print('Страница', page, r_j)
                            if r_json['status'] == 'ok':
                                count = 0
                                # Проходим по записям в json
                                for i in r_json['result']['items']:
                                    # Проверяем является ли запись объявлением
                                    if i['type'] == 'item':
                                        try:
                                            r_o = s.request('GET',
                                                            f'https://m.avito.ru/api/18/items/{i["value"]["id"]}?key=af0deccbgcgidddjgnvljitntccdduijhdinfgjgfjir',
                                                            headers=headers[rd.randint(0, 9)], timeout=60)
                                            r_order = json.loads(replacer(r_o.text))
                                            try:
                                                # Обновляем список со всеми параметрами объявлений
                                                [firebase_new.add(i) for i in r_order['firebaseParams'].keys() if i not in firebase]
                                                pars = {i['title']: i['description'] for i in r_order['parameters']['flat']}
                                                [params_new.add(i) for i in pars.keys() if i not in params]
                                                # Создаем записть в словаре с объявлениями
                                                orders[str(i['value']['id'])] = {
                                                    'title': i['value']['title'],
                                                    'city': city_rus,
                                                    'url': 'https://www.avito.ru' + i['value'].get('uri_mweb', None),
                                                    'normalizedPrice': i['value'].get('normalizedPrice', None).split('₽')[0].replace(' ', ''),
                                                    'address': r_order.get('address', None),
                                                    'county': (r_order['geoReferences'][0].get('content', None)
                                                               if r_order['geoReferences'] else '')
                                                }
                                                for item in firebase:
                                                    orders[str(i['value']['id'])][item] = r_order['firebaseParams'].get(item, None)
                                                for item2 in params:
                                                    orders[str(i['value']['id'])][item2] = pars.get(item2, None)
                                                try:
                                                    min_price = r_order['firebaseParams'].get('itemPrice')
                                                except Exception:
                                                    print('Не удалось обновить минимальную цену')
                                                count += 1
                                            except Exception:
                                                not_add += 1
                                        except Exception:
                                            print('Ошибка при проходе по объявлению ', Exception)
                                            time.sleep(rd.randint(20, 30))
                                            count_except_orders += 1
                                    else:
                                        skip.append(i['type'])
                            else:
                                count_no_ok += 1
                        except Exception:
                            time.sleep(rd.randint(20, 30))
                            count_except_pages += 1
                        # Проверяем следующуюю страницу, есть ли на ней объявления
                        check2 = s.request('GET', f'https://m.avito.ru/api/11/items?key=af0deccbgcgidddjgnvljitntccdduijhdinfgjgfjir&params{categories[cat]["param"]}&categoryId={cat}&locationId={location_id}&page={page+1}&priceAsc&priceMin={curent_min_price}&lastStamp=1660769220&display=list&limit=50',
                                                headers=headers[rd.randint(0, 9)], timeout=60)
                        check2_json = json.loads(replacer(check2.text))
                        if check2_json['result']['items']:
                            flag2 = True
                            page += 1
                        else:
                            flag2 = False
                    # Если в предыдущей проверке оказалось, что на следующей странице объявлений нет, то проверяем
                    # новый json с увеличенным значением минимальной цены
                    check = s.request('GET',
                                      f'https://m.avito.ru/api/11/items?key=af0deccbgcgidddjgnvljitntccdduijhdinfgjgfjir&params{categories[cat]["param"]}&categoryId={cat}&locationId={location_id}&page=1&priceAsc&priceMin={min_price}&lastStamp=1660769220&display=list&limit=50',
                                      headers=headers[rd.randint(0, 9)], timeout=60)
                    print('check url: ', f'https://m.avito.ru/api/11/items?key=af0deccbgcgidddjgnvljitntccdduijhdinfgjgfjir&params{categories[cat]["param"]}&categoryId={cat}&locationId={location_id}&page=1&priceAsc&priceMin={min_price}&lastStamp=1660769220&display=list&limit=50')
                    check_json = json.loads(replacer(check.text))
                    if int(check_json['result']['mainCount']) > (len([i['type'] for i in check_json['result']['items']
                                                                      if i['type'] == 'mapBanner'
                                                                         or i['type'] == 'xlItem'])):
                        flag = True
                        print('check: ', flag)
                    else:
                        flag = False
                        print('check: ', flag)

        except Exception:
            count_except_city += 1
            time.sleep(rd.randint(20, 30))

        # Записываем все собранные объявления в json для города
        with open(f'items-{loc}.json', 'w') as file:
            json.dump(orders, file, indent=4, ensure_ascii=False)
        # Добавляем все собранные объявления в общий json
        if os.path.exists('items-all.json'):
            with open('items-all.json', 'r', encoding='utf-8') as file:
                all_orders = json.load(file)
                all_orders.update(orders)
            with open('items-all.json', 'w', encoding='utf-8') as file:
                json.dump(all_orders, file, indent=4, ensure_ascii=False)
        else:
            with open('items-all.json', 'w', encoding='utf-8') as file:
                all_orders = orders
                json.dump(all_orders, file, indent=4, ensure_ascii=False)
        # Записываем в "прогресс" результаты прохода по городу
        with open('progress.txt', 'a') as file:
            file.write(f'Прогон по городу {city_rus} завершен\nВсего добавлено {len(orders)}\n'
                        f'Всего в общем файле: {len(all_orders)}\n')

    print('Ошибок при обходе городов: ', count_except_city)
    print('Ошибок при обходе pages: ', count_except_pages)
    print('Ошибок при обходе объявлений города: ', count_except_orders)
    print('Ошибок статуса: ', count_no_ok)
    print('Не добавлено карточек: ', not_add)
    print('Пропущено объявлений неверного типа: ', len(skip))
    print('Пропущеные типы: ', set(skip))
    print('Список firebase: ', firebase_new)
    print('  Количество: ', len(firebase_new))
    print('Список params: ', params_new)
    print('  Количество: ', len(params_new))
    with open('progress.txt', 'a') as file:
        file.write(f'{"-"*40}\n'
                   f'Ошибок при обходе городов: {count_except_city}\n'
                   f'Ошибок при обходе pages: {count_except_pages}\n'
                   f'Ошибок при обходе объявлений города: {count_except_orders}\n'
                   f'Ошибок статуса: {count_no_ok}\n'
                   f'Не добавлено карточек: {not_add}\n'
                   f'Пропущено объявлений неверного типа: {len(skip)}\n'
                   f'Пропущеные типы: {set(skip)}\n'
                   f'Список firebase: {firebase_new}\n'
                   f'Количество: {len(firebase_new)}\n'
                   f'Список params: {params_new}\n'
                   f'Количество: {len(params_new)}\n')


if __name__ == '__main__':
    start_parse()
