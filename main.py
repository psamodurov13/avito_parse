# -*- coding: utf-8 -*-
import json
import os.path
import random
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
#     # 'https': 'http://--.---.---.---:8000'
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
    # cookie and other headers from browser
    cookie = ''
    user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Safari/605.1.15'
    accept = 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
    if_none_match = 'W/"1a51ed-dfV3WgFMzOo848sSZY4kq1SizdM"'
    accept_encoding = 'gzip, deflate, br'
    headers = {
        'Accept': accept,
        'Cookie': cookie,
        'Accept-Encoding': accept_encoding,
        'User-agent': user_agent,
        'If-None-Match': if_none_match
    }
    s.headers = headers
    logger.info(s.headers)
    # create random headers
    # header = Headers(
    #     browser="chrome",  # Generate only Chrome UA
    #     # os="linux",  # Generate only ___ platform
    #     headers=True  # generate misc headers
    # )
    # headers = [header.generate() for i in range(10)]
    # for i in headers:
    #     i['Cookie'] = cookie

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
        file.write(f'Start {str(datetime.datetime.now())}\n')

    a = s.get(f'https://www.avito.ru/', timeout=20)
    logger.info(a)
    # Iterate regions
    for loc in locs:
        orders = {}
        city_rus = ''
        # Iterate categories
        try:
            for cat in categories:
                r = s.get(f'https://www.avito.ru/{loc}/{categories[cat]["url"]}/prodam-ASgBAgICAUSSA8YQ?p=1&presentationType=serp',
                              timeout=20)
                logger.info(f'Category {r}')
                # Get information about region
                soup = bs(r.text, features='html.parser')
                loc_index = r.text.find('locationId=') + 11
                location_id = r.text[loc_index:loc_index + 6]
                city_rus = soup.select_one('.breadcrumbs-linkWrapper-jZP0j:first-child a').text
                pages = soup.select_one('.pagination-root-Ntd_O .pagination-item-JJq_j:nth-last-child(2)').text
                logger.info(f'{"-"*40}\nCity: {str(city_rus)}\nTotal pages: {pages}\n')
                # Write information in progress.txt
                with open('progress.txt', 'a') as file:
                    file.write(f'{"-"*40}\nCity: {str(city_rus)}\nTotal pages: {pages}\n')
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
                            r_j = s.get(f'https://m.avito.ru/api/11/items?key=af0deccbgcgidddjgnvljitntccdduijhdinfgjgfjir&params{categories[cat]["param"]}&categoryId={cat}&locationId={location_id}&page={page}&sort=priceAsc&priceMin={curent_min_price}&lastStamp=1660769220&display=list&limit=50',
                                        timeout=60)
                            r_json = json.loads(replacer(r_j.text))
                            logger.info(f'Page {page}, {r_j}')
                            if r_json['status'] == 'ok':
                                count = 0
                                # Iterate items in JSON
                                for i in r_json['result']['items']:
                                    # is the item an ad?
                                    if i['type'] == 'item':
                                        try:
                                            r_o = s.get(f'https://m.avito.ru/api/18/items/{i["value"]["id"]}?key=af0deccbgcgidddjgnvljitntccdduijhdinfgjgfjir',
                                                        timeout=60)
                                            r_order = json.loads(replacer(r_o.text))
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
                                                except Exception:
                                                    logger.exception('Minimal price not update')
                                                logger.info(orders[str(i['value']['id'])])
                                                logger.info(f'PROCESSED ON PAGE {page}/{curent_min_price} - {count}')
                                                count += 1
                                                time.sleep(random.randint(5, 20) * 0.1)
                                            except Exception:
                                                logger.exception('Not add')
                                                not_add += 1
                                        except Exception:
                                            logger.exception('Exception on ad page')
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
                        check2 = s.get( f'https://m.avito.ru/api/11/items?key=af0deccbgcgidddjgnvljitntccdduijhdinfgjgfjir&params{categories[cat]["param"]}&categoryId={cat}&locationId={location_id}&page={page+1}&priceAsc&priceMin={curent_min_price}&lastStamp=1660769220&display=list&limit=50',
                                        timeout=60)
                        check2_json = json.loads(replacer(check2.text))
                        if check2_json['result']['items']:
                            flag2 = True
                            page += 1
                        else:
                            flag2 = False

                    # If in the previous check it turned out that there are no ads on the next page, then we check
                    # the new json with an increased value of the minimum price
                    check = s.get(f'https://m.avito.ru/api/11/items?key=af0deccbgcgidddjgnvljitntccdduijhdinfgjgfjir&params{categories[cat]["param"]}&categoryId={cat}&locationId={location_id}&page=1&priceAsc&priceMin={min_price}&lastStamp=1660769220&display=list&limit=50',
                                  timeout=60)
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
            file.write(f'City {city_rus} done\nTotal added {len(orders)}\n'
                        f'Total in main file: {len(all_orders)}\n')

    logger.info(f'City exceptions: {count_except_city}')
    logger.info(f'City page exceptions: {count_except_pages}')
    logger.info(f'Ad page exceptions: {count_except_orders}')
    logger.info(f'Status exceptions: {count_no_ok}')
    logger.info(f'Not added ads: {not_add}')
    logger.info(f'Ads with wrong type: {len(skip)}')
    logger.info(f'Skiped type: {set(skip)}')
    logger.info(f'List firebase: {firebase_new}')
    logger.info(f'  Quantity: {len(firebase_new)}')
    logger.info(f'List params: {params_new}')
    logger.info(f'  Quantity: {len(params_new)}')
    with open('progress.txt', 'a') as file:
        file.write(f'{"-"*40}\n'
                   f'City exceptions: {count_except_city}\n'
                   f'City page exceptions: {count_except_pages}\n'
                   f'Ad page exceptions: {count_except_orders}\n'
                   f'Status exceptions: {count_no_ok}\n'
                   f'Not added ads: {not_add}\n'
                   f'Ads with wrong type: {len(skip)}\n'
                   f'Skiped type: {set(skip)}\n'
                   f'List firebase: {firebase_new}\n'
                   f'  Quantity: {len(firebase_new)}\n'
                   f'List params: {params_new}\n'
                   f'  Quantity: {len(params_new)}\n')


if __name__ == '__main__':
    start_parse()
