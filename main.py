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

logger.add('debug.log', format='{time} {level} {message}', level='INFO', rotation='15 MB', compression='zip')
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
    # cookie from browser
    cookie = 'v=1670596614; buyer_from_page=main; _buzz_fpc=JTdCJTIycGF0aCUyMiUzQSUyMiUyRiUyMiUyQyUyMmRvbWFpbiUyMiUzQSUyMi53d3cuYXZpdG8ucnUlMjIlMkMlMjJleHBpcmVzJTIyJTNBJTIyU2F0JTJDJTIwMDklMjBEZWMlMjAyMDIzJTIwMTQlM0EzNiUzQTU5JTIwR01UJTIyJTJDJTIyU2FtZVNpdGUlMjIlM0ElMjJMYXglMjIlMkMlMjJ2YWx1ZSUyMiUzQSUyMiU3QiU1QyUyMnZhbHVlJTVDJTIyJTNBJTVDJTIyMzIxZDc0YmFjNjA4ZDhmMThkZWZkZWMxZDI0Y2FlOGQlNUMlMjIlMkMlNUMlMjJmcGpzRm9ybWF0JTVDJTIyJTNBdHJ1ZSU3RCUyMiU3RA==; tmr_detect=0%7C1670596619848; _ym_isad=2; _ym_visorc=b; cto_bundle=AX775F9kbTVucllFOGpOREJMMzhYeHNSJTJGOU5wTDhwd1FsMU95YzdoNEhpbm94a2h4WTRCRzVXaXd4ZzExU3ZuUmM4Z0hnMUdjZUVDbEZsMkM2ZyUyQnZtaUZiT2dvJTJCOHFoejBSRDVuajJsTGxZMFcwb05rVGVKcmxPZ1hwU282VFlNckJJQw; tmr_lvid=e6c4720876546a9a8a2b032d951a5691; tmr_lvidTS=1670426142793; u=2tb1rorv.1kt7bx.q11hrzywz900; _ga=GA1.1.214193898.1670426142; _ga_M29JC28873=GS1.1.1670596616.12.0.1670596616.60.0.0; abp=0; buyer_laas_location=658080; buyer_location_id=658080; dfp_group=32; isLegalPerson=0; luri=tula; sx=H4sIAAAAAAAC%2F1TQUXKrMAyF4b34OQ%2ByEJKV3WDZQEOISUnigQ57v3Mf0mk28J1%2Fzo8jjD5qlgYoYxTULD70HVIvyXcduvOPe7mzw9dGz%2B%2FL0rbz5Ps7j%2Fel%2F3qNabnGsKebO7nszp4FmAO37XFyzMyWhHtlbZlYs8TcaJIWzCTpW95j2oZxlvWJa13W%2BeEXwl3teW%2Bv8638kUMDno6Tk7uK0p4vD1sNAhSroRjU8CYV1Tj7zLE3TBEZOxTLnEA8WyZJFBGi%2FK3WFvE4OQXPGWLDPXiF3LBao%2BZZNRtiz78TEtN0EZrk6zrHMBBBeZQ0htf0nWb6%2FCP8r7a9G6HbtlsYJgMoK9A0Deta3%2BSFazcabH6fwlqs1hrWSlaGoVAB%2B4hlao%2FjXwAAAP%2F%2Fz%2BufFb4BAAA%3D; SEARCH_HISTORY_IDS=4%2C1; f=5.cc913c231fb04ced4b5abdd419952845a68643d4d8df96e9a68643d4d8df96e9a68643d4d8df96e9a68643d4d8df96e94f9572e6986d0c624f9572e6986d0c624f9572e6986d0c62ba029cd346349f36c1e8912fd5a48d02c1e8912fd5a48d0246b8ae4e81acb9fa143114829cf33ca746b8ae4e81acb9fad99271d186dc1cd062a5120551ae78edaf305aadb1df8ceb48bdd0f4e425aba7085d5b6a45ae867377bdb88c79240d01ff38e8d292af81e50df103df0c26013a2ebf3cb6fd35a0ac71e7cb57bbcb8e0ff0c77052689da50ddc5322845a0cba1aba0ac8037e2b74f92da10fb74cac1eab2da10fb74cac1eab2da10fb74cac1eabdc5322845a0cba1a0df103df0c26013a1d6703cbe432bc2a9302348a0777e4ca2701bf0b8152ebed721b24f48ff3b002f7f4d5e422becca502c730c0109b9fbb7b2f02252ec391a44a3d0ad59421601eb9abe6099f96bacbf72316305726b23530c4db039d665204e2415097439d404746b8ae4e81acb9fa786047a80c779d5146b8ae4e81acb9fa455c843368c8d310b6c9122eda0b0e572da10fb74cac1eaba5f76aa56199c549efde06a8e6b1dd6f704a8898bb3a47d43a74862e550dfed7; ft="cvuFu342fPK8X07N3kdHH/uub//JS/DG5MUI1Uo2rf+H9K+a7KdYlwDc9+jFtubpU2jAYnwLVnoGHA76NxPRcxbNzrUGl17h9J10q1grEolsYMxi2z5tEtTPqy53OcUH5lQ57ncUeynF9tNeo0Qykr/Xd7wA5RwAvEc+i0sDosMn3mK7roOO0BzJyE/xXZjl"; uxs_uid=09e70e90-7642-11ed-aa16-93d527e39e87; adrcid=A-Skuze_F9Joewo6LHmV8fQ; _ym_d=1670426144; _ym_uid=1670426144131501060; _gcl_au=1.1.1495186705.1670426142; auth=1; sessid=16e153e40effca8cbda1a5f1eb152443.1660772242'
    # create random headers
    header = Headers(
        browser="chrome",  # Generate only Chrome UA
        # os="linux",  # Generate only ___ platform
        headers=True  # generate misc headers
    )
    headers = [header.generate() for i in range(10)]
    for i in headers:
        i['Cookie'] = cookie

    # List of regions
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

    # Categories for parsing
    categories = {
        # '25': {'url': 'doma_dachi_kottedzhi', 'param': '[202]=1064'},
        '24': {'url': 'kvartiry', 'param': '[201]=1059'}
    }
    # Parameters for parsing
    firebase = ['itemPrice', 'area', 'area_kitchen', 'area_live', 'commission',
                'floor', 'floors_count', 'house_type', 'nazvanie_obekta_nedvizhimosti', 'ofitsialnyy_zastroyshchik',
                'otdelka', 'rooms', 'status', 'type', 'categorySlug', 'distance_to_city', 'house_area', 'material_sten',
                'site_area', 'god_postroiki']
    params = ['Количество комнат', 'Статус', 'Балкон или лоджия', 'Тип комнат', 'Высота потолков, м', 'Санузел', 'Окна',
              'Отделка', 'Способ продажи', 'Коммуникации', 'Ремонт', 'Парковка', 'Мебель', 'Техника', 'Инфраструктура',
              'Терраса или веранда']
    # variables for collecting summary information
    count_except_city = 0
    skip = []
    count_no_ok = 0
    count_except_orders = 0
    count_except_pages = 0
    firebase_new = set()
    params_new = set()
    not_add = 0

    # Create report
    with open('progress.txt', 'w') as file:
        file.write(f'Запущено {str(datetime.datetime.now())}\n')

    # Iterate regions
    for loc in locs:
        orders = {}
        city_rus = ''
        # Iterate categories
        try:
            for cat in categories:
                r = s.request('GET',
                              f'https://www.avito.ru/{loc}/{categories[cat]["url"]}/prodam-ASgBAgICAUSSA8YQ?p=1&presentationType=serp',
                              headers=headers[rd.randint(0, 9)], timeout=60)
                # Get information about region
                soup = bs(r.text, features='html.parser')
                loc_index = r.text.find('locationId=') + 11
                location_id = r.text[loc_index:loc_index + 6]
                city_rus = soup.select_one('.breadcrumbs-linkWrapper-jZP0j:first-child a').text
                pages = soup.select_one('.pagination-root-Ntd_O .pagination-item-JJq_j:nth-last-child(2)').text
                logger.info(f'{"-"*40}\nГород: {str(city_rus)}\nСтраниц всего: {pages}\n')
                # Write information in progress.txt
                with open('progress.txt', 'a') as file:
                    file.write(f'{"-"*40}\nГород: {str(city_rus)}\nСтраниц всего: {pages}\n')
                time.sleep(2)
                min_price = 0
                # Iterate pages of category
                flag = True
                while flag:
                    page = 1
                    curent_min_price = min_price
                    # After iterate 100 pages update value of "curent_min_price" and iterate new pages
                    flag2 = True
                    while flag2:
                        try:
                            r_j = s.request('GET', f'https://m.avito.ru/api/11/items?key=af0deccbgcgidddjgnvljitntccdduijhdinfgjgfjir&params{categories[cat]["param"]}&categoryId={cat}&locationId={location_id}&page={page}&sort=priceAsc&priceMin={curent_min_price}&lastStamp=1660769220&display=list&limit=50',
                                            headers=headers[rd.randint(0, 9)], timeout=60)
                            r_json = json.loads(replacer(r_j.text))
                            logger.info(f'Страница {page}, {r_j}')
                            if r_json['status'] == 'ok':
                                # logger.info(r_json['status'])
                                count = 0
                                # Iterate items in JSON
                                for i in r_json['result']['items']:
                                    # is the item an ad?
                                    if i['type'] == 'item':
                                        # logger.info(i['type'])
                                        try:
                                            r_o = s.request('GET',
                                                            f'https://m.avito.ru/api/18/items/{i["value"]["id"]}?key=af0deccbgcgidddjgnvljitntccdduijhdinfgjgfjir',
                                                            headers=headers[rd.randint(0, 9)], timeout=60)
                                            r_order = json.loads(replacer(r_o.text))
                                            # logger.info(r_o)
                                            logger.info(f'https://m.avito.ru/api/18/items/{i["value"]["id"]}?key=af0deccbgcgidddjgnvljitntccdduijhdinfgjgfjir')
                                            try:
                                                # Update list with ad's parameters
                                                [firebase_new.add(i) for i in r_order['firebaseParams'].keys() if i not in firebase]
                                                pars = {i['title']: i['description'] for i in r_order['parameters']['flat']}
                                                [params_new.add(i) for i in pars.keys() if i not in params]
                                                # Create item in a dict with ads
                                                orders[str(i['value']['id'])] = {
                                                    'title': i['value']['title'],
                                                    'city': city_rus,
                                                    'url': 'https://www.avito.ru' + i['value'].get('uri_mweb', None),
                                                    'normalizedPrice': i['value'].get('normalizedPrice', None).split('₽')[0].replace(' ', ''),
                                                    'address': r_order.get('address', None),
                                                    'county': (r_order['geoReferences'][0].get('content', None)
                                                               if 'geoReferences' in r_order.keys() else '')
                                                }
                                                for item in firebase:
                                                    orders[str(i['value']['id'])][item] = r_order['firebaseParams'].get(item, None)
                                                for item2 in params:
                                                    orders[str(i['value']['id'])][item2] = pars.get(item2, None)
                                                try:
                                                    min_price = r_order['firebaseParams'].get('itemPrice')
                                                    # logger.info(f'MIN PRICE - {min_price}')
                                                except Exception:
                                                    logger.exception('Не удалось обновить минимальную цену')
                                                logger.info(orders[str(i['value']['id'])])
                                                logger.info(f'PROCESSED ON PAGE {page}/{curent_min_price} - {count}')
                                                count += 1
                                            except Exception:
                                                logger.exception('Not add')
                                                not_add += 1
                                        except Exception:
                                            logger.exception('Ошибка при проходе по объявлению ')
                                            time.sleep(rd.randint(20, 30))
                                            count_except_orders += 1
                                    else:
                                        skip.append(i['type'])
                            else:
                                count_no_ok += 1
                        except Exception:
                            time.sleep(rd.randint(20, 30))
                            count_except_pages += 1
                        # Check next page of category
                        check2 = s.request('GET', f'https://m.avito.ru/api/11/items?key=af0deccbgcgidddjgnvljitntccdduijhdinfgjgfjir&params{categories[cat]["param"]}&categoryId={cat}&locationId={location_id}&page={page+1}&priceAsc&priceMin={curent_min_price}&lastStamp=1660769220&display=list&limit=50',
                                                headers=headers[rd.randint(0, 9)], timeout=60)
                        check2_json = json.loads(replacer(check2.text))
                        if check2_json['result']['items']:
                            flag2 = True
                            page += 1
                        else:
                            flag2 = False

                    # If in the previous check it turned out that there are no ads on the next page, then we check
                    # the new json with an increased value of the minimum price
                    check = s.request('GET',
                                      f'https://m.avito.ru/api/11/items?key=af0deccbgcgidddjgnvljitntccdduijhdinfgjgfjir&params{categories[cat]["param"]}&categoryId={cat}&locationId={location_id}&page=1&priceAsc&priceMin={min_price}&lastStamp=1660769220&display=list&limit=50',
                                      headers=headers[rd.randint(0, 9)], timeout=60)
                    logger.info(f'check url: https://m.avito.ru/api/11/items?key=af0deccbgcgidddjgnvljitntccdduijhdinfgjgfjir&params{categories[cat]["param"]}&categoryId={cat}&locationId={location_id}&page=1&priceAsc&priceMin={min_price}&lastStamp=1660769220&display=list&limit=50')
                    check_json = json.loads(replacer(check.text))
                    if int(check_json['result']['mainCount']) > (len([i['type'] for i in check_json['result']['items']
                                                                      if i['type'] == 'mapBanner'
                                                                         or i['type'] == 'xlItem'])):
                        flag = True
                        logger.info(f'check: {flag}')
                    else:
                        flag = False
                        logger.info(f'check: {flag}')

        except Exception:
            count_except_city += 1
            time.sleep(rd.randint(20, 30))

        # Write all collected ads in json for city
        with open(f'items-{loc}.json', 'w') as file:
            json.dump(orders, file, indent=4, ensure_ascii=False)
        # Write all collected ads in common json
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
        # Write results in progress.txt
        with open('progress.txt', 'a') as file:
            file.write(f'Прогон по городу {city_rus} завершен\nВсего добавлено {len(orders)}\n'
                        f'Всего в общем файле: {len(all_orders)}\n')

    logger.info(f'Ошибок при обходе городов: {count_except_city}')
    logger.info(f'Ошибок при обходе pages: {count_except_pages}')
    logger.info(f'Ошибок при обходе объявлений города: {count_except_orders}')
    logger.info(f'Ошибок статуса: {count_no_ok}')
    logger.info(f'Не добавлено карточек: {not_add}')
    logger.info(f'Пропущено объявлений неверного типа: {len(skip)}')
    logger.info(f'Пропущеные типы: {set(skip)}')
    logger.info(f'Список firebase: {firebase_new}')
    logger.info(f'  Количество: {len(firebase_new)}')
    logger.info(f'Список params: {params_new}')
    logger.info(f'  Количество: {len(params_new)}')
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
