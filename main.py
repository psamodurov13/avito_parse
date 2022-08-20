import json
import os.path
import ssl
import requests as rq
import time
import random as rd
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.poolmanager import PoolManager
from requests.packages.urllib3.util import ssl_
from bs4 import BeautifulSoup as bs
from loguru import logger

logger.add('debug.log', format='{time} {level} {message}', level='DEBUG', rotation='10 KB', compression='zip')

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

    headers = {
        'Cookie': 'tmr_reqNum=75; buyer_from_page=catalog; v=1660772228; tmr_detect=0%7C1660773411723; _ym_visorc=b; cto_bundle=5oEQuF9YbkVWWXZhaG45JTJCcnFEeWROWTNNQjFQSnpCa1c4V0hMTnY3ZlNxZ3dEeW9DJTJCa29yR282JTJGelE0YmxkNHQzWjVkc2JOTlVpYUZHcyUyQmZCMjNkbXJ6SlBBZTBoWGF0aXg2eXJVVWFibUdxR3M4dzNjR0loMHNaSTRKTiUyRkRpSVI2eFU; _buzz_fpc=JTdCJTIycGF0aCUyMiUzQSUyMiUyRiUyMiUyQyUyMmRvbWFpbiUyMiUzQSUyMi53d3cuYXZpdG8ucnUlMjIlMkMlMjJleHBpcmVzJTIyJTNBJTIyVGh1JTJDJTIwMTclMjBBdWclMjAyMDIzJTIwMjElM0E1NiUzQTQ5JTIwR01UJTIyJTJDJTIyU2FtZVNpdGUlMjIlM0ElMjJMYXglMjIlMkMlMjJ2YWx1ZSUyMiUzQSUyMiU3QiU1QyUyMnZhbHVlJTVDJTIyJTNBJTVDJTIyOTUzMjlkNDhmNTY0ZjkyZGE0MDYyOTRjZmQ1MmJhMDUlNUMlMjIlMkMlNUMlMjJmcGpzRm9ybWF0JTVDJTIyJTNBdHJ1ZSU3RCUyMiU3RA==; SEARCH_HISTORY_IDS=1; _ga=GA1.2.235849490.1660735646; _ga_9E363E7BES=GS1.1.1660772231.6.1.1660773406.36.0.0; _ga_M29JC28873=GS1.1.1660772231.6.1.1660773406.36.0.0; _gid=GA1.2.747418574.1660735646; buyer_location_id=658080; sx=H4sIAAAAAAAC%2F1TQTbKrIBBA4b0wzgAaaOjsBhr8w2i8RI255d5fvYFVuRv46tT5FcYDuuABdFJR2gRspJYMIUlQQCzuv2ITd%2FEOj6WdXiU8it2PoRvtUj9qYIAH93MrbiKLu0KUHkCiPG8CEZGTw4aQLBqk7GLWlJyVzC7RJeOy14859NEe61sWxOc2zdOhAvrJzfZLtuQ8nDfhAKIj65IOSTWOkVkGCg05L41hvORPt2q1NdG0I42R3xP8IPZj6ZYu0bT9bTbqvIloVeuX5267YnxlWUuprd%2BNv8iHmWPJfjEYnoPp9bzXI856WmYKMudvkqT6T2aJ3ulk2MaM7Mg7yBEIyBK5YPQlzwtMPcBLk39aWpdD%2BSGNcX28O%2BxH%2FrPBS3veRF9p21671EOp7NvSsmTv9%2FkSzadQNznbfxpXuSs%2Fq6%2F5hdPAzZoTfolao8bz%2FBcAAP%2F%2Fxfnx%2BQ4CAAA%3D; tmr_lvid=271492dcdddbfdec9307ff12884845d5; tmr_lvidTS=1659474263915; abp=0; redirectMav=1; buyer_laas_location=658080; dfp_group=22; isLegalPerson=0; u=2tb1rorv.1kt7bx.q11hrzywz900; auth=1; sessid=16e153e40effca8cbda1a5f1eb152443.1660772242; f=5.cc913c231fb04ced4b5abdd419952845a68643d4d8df96e9a68643d4d8df96e9a68643d4d8df96e9a68643d4d8df96e94f9572e6986d0c624f9572e6986d0c624f9572e6986d0c62ba029cd346349f36c1e8912fd5a48d02c1e8912fd5a48d0246b8ae4e81acb9fa143114829cf33ca746b8ae4e81acb9fad99271d186dc1cd062a5120551ae78edaf305aadb1df8ceb48bdd0f4e425aba7085d5b6a45ae867377bdb88c79240d01ff38e8d292af81e50df103df0c26013a2ebf3cb6fd35a0ac71e7cb57bbcb8e0ff0c77052689da50ddc5322845a0cba1aba0ac8037e2b74f92da10fb74cac1eab2da10fb74cac1eab2da10fb74cac1eabdc5322845a0cba1a0df103df0c26013a1d6703cbe432bc2a9302348a0777e4ca2701bf0b8152ebed721b24f48ff3b002f7f4d5e422becca502c730c0109b9fbbf3201ab92805fd167f743a655165e6dfb9abe6099f96bacbf72316305726b23530c4db039d665204e2415097439d404746b8ae4e81acb9fa786047a80c779d5146b8ae4e81acb9fae5db0314879dff868edb85158dee9a662da10fb74cac1eaba5f76aa56199c549efde06a8e6b1dd6f704a8898bb3a47d43a74862e550dfed7; ft="YDfyTMrZevgmPnxVUnKQA1F5dVdhtI/1ECZCxfS6Gp8DF/beVucFOw0h0lCGgVNO+Ec6+NfZ96+6IaFBoVNCnbaNrieecr9vKwQ+iUJWHFIyHNv5PvBSVHVrfRig0tiXUnDBpDIeS/PGifXb9oIDpBqsgucSddLwcWC14PzVl8D8AeQqUTij0AdfkHCuInHc"; adrcid=AGqrmznCPBr44-g6MgMpWLQ; adrdel=1; _ym_isad=2; _gcl_au=1.1.1295856914.1660735646; _ym_d=1660735646; _ym_uid=1659474263557749019; luri=tula',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        # 'Host': 'www.avito.ru',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.6 Safari/605.1.15',
        'Accept-Language': 'ru',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive'
    }
    locs = ['tula', 'voronezh', 'moskva']

    categories = {
        '25': {'url': 'doma_dachi_kottedzhi', 'param': '[202]=1064'},
        '24': {'url': 'kvartiry', 'param': '[201]=1059'}
    }
    firebase = ['itemPrice', 'area', 'area_kitchen', 'area_live', 'commission',
                'floor', 'floors_count', 'house_type', 'nazvanie_obekta_nedvizhimosti', 'ofitsialnyy_zastroyshchik',
                'otdelka', 'rooms', 'status', 'type', 'categorySlug', 'distance_to_city', 'house_area', 'material_sten',
                'site_area', 'god_postroiki']
    params = ['Количество комнат', 'Статус', 'Балкон или лоджия', 'Тип комнат', 'Высота потолков, м', 'Санузел', 'Окна',
              'Отделка', 'Способ продажи', 'Коммуникации', 'Ремонт', 'Парковка', 'Мебель', 'Техника', 'Инфраструктура',
              'Терраса или веранда']
    count_except_city = 0
    skip = []
    count_no_ok = 0
    count_except_orders = 0
    count_except_pages = 0
    firebase_new = set()
    params_new = set()
    not_add = 0

    for loc in locs:
        orders = {}
        city_rus = ''
        try:
            for cat in categories:
                r = s.request('GET',
                              f'https://www.avito.ru/{loc}/{categories[cat]["url"]}/prodam-ASgBAgICAUSSA8YQ?p=1&presentationType=serp',
                              headers=headers)
                print(r)
                soup = bs(r.text, features='html.parser')
                loc_index = r.text.find('locationId=') + 11
                location_id = r.text[loc_index:loc_index + 6]
                print('LocID: ', location_id)
                city_rus = soup.select_one('.breadcrumbs-linkWrapper-jZP0j:first-child a').text
                print('City: ', city_rus)
                print('Cat: ', cat, categories[cat])
                pages = soup.select_one('.pagination-root-Ntd_O .pagination-item-JJq_j:nth-last-child(2)').text
                print('Страниц всего: ', pages)
                time.sleep(2)
                min_price = 0
                flag = True
                while flag:
                    print('start flag1')
                    page = 1
                    curent_min_price = min_price
                    flag2 = True
                    while flag2:
                        print('Страница: ', page, f'минимальная цена {min_price}')
                        try:
                            r_j = s.request('GET',
                                            f'https://m.avito.ru/api/11/items?key=af0deccbgcgidddjgnvljitntccdduijhdinfgjgfjir&params{categories[cat]["param"]}&categoryId={cat}&locationId={location_id}&page={page}&sort=priceAsc&priceMin={curent_min_price}&lastStamp=1660769220&display=list&limit=50',
                                            headers=headers)
                            r_json = json.loads(replacer(r_j.text))
                            print(f'Всего объявлений в json {r_json["result"]["mainCount"]}')
                            # Дополнительные параметры json &pageId=H4sIAAAAAAAA_0q0MrSqLrYyNLRSKskvScyJT8svzUtRss60MjG1NLKuBQQAAP__VFgqayAAAAA&presentationType=serp
                            if r_json['status'] == 'ok':
                                print('r_json-status', r_json['status'])
                                count = 0
                                for i in r_json['result']['items']:
                                    if i['type'] == 'item':
                                        try:
                                            r_o = s.request('GET',
                                                            f'https://m.avito.ru/api/18/items/{i["value"]["id"]}?key=af0deccbgcgidddjgnvljitntccdduijhdinfgjgfjir',
                                                            headers=headers)
                                            r_order = json.loads(replacer(r_o.text))
                                            try:
                                                [firebase_new.add(i) for i in r_order['firebaseParams'].keys() if i not in firebase]
                                                # print('fb-new', firebase_new)
                                                pars = {i['title']: i['description'] for i in r_order['parameters']['flat']}
                                                # print('pars', pars)
                                                [params_new.add(i) for i in pars.keys() if i not in params]
                                                # print('params', params)
                                                orders[str(i['value']['id'])] = {
                                                    'title': i['value']['title'],
                                                    'city': city_rus,
                                                    'url': 'https://www.avito.ru' + i['value'].get('uri_mweb', 'NA'),
                                                    'normalizedPrice': i['value'].get('normalizedPrice', 'NA').split('₽')[0].replace(' ', ''),
                                                    'address': r_order.get('address', 'NA'),
                                                    'county': (r_order['geoReferences'][0].get('content', 'NA')
                                                               if r_order['geoReferences'] else '')
                                                }
                                                print(orders[str(i['value']['id'])])

                                                # print(orders)
                                                for item in firebase:
                                                    orders[str(i['value']['id'])][item] = r_order['firebaseParams'].get(item, 'NA')
                                                # print(orders)
                                                for item2 in params:
                                                    orders[str(i['value']['id'])][item2] = pars.get(item2, 'NA')
                                                # print(orders[str(i['value']['id'])])
                                                # time.sleep(rd.randint(0, 2))
                                                # orders[i['value']['id']]['value']['city'] = city_rus
                                                # print(f"Добавлен {i['value']['id']}")
                                                try:
                                                    min_price = r_order['firebaseParams'].get('itemPrice')
                                                    print('Минимальная цена', min_price)
                                                    print('min_price: ', min_price)
                                                    print('curent_min_price: ', curent_min_price)
                                                except Exception:
                                                    print('Не удалось обновить минимальную цену')
                                                count += 1
                                                print(f'{city_rus}, {cat}, страница - {page}, объявлений - {count}')
                                            except Exception:
                                                print('Карточка не добавлена')
                                                # time.sleep(rd.randint(20, 30))
                                                not_add += 1
                                                print(r_order)
                                        except Exception:
                                            print('Ошибка при проходе по объявлению ', Exception)
                                            time.sleep(rd.randint(20, 30))
                                            count_except_orders += 1
                                    else:
                                        skip.append(i['type'])
                                        print(f"Пропущен {i['type']}")

                                print('Добавлено: ', count, ' объявлений')
                                print('Всего добавлено: ', len(orders), ' объявлений')


                                # time.sleep(rd.randint(2, 5))
                            else:
                                print('Загрузка не удалась, статус не ок')
                                count_no_ok += 1
                        except Exception:
                            print('Ошибка при проходе по page', Exception)
                            time.sleep(rd.randint(20, 30))
                            count_except_pages += 1

                        check2 = s.request('GET', f'https://m.avito.ru/api/11/items?key=af0deccbgcgidddjgnvljitntccdduijhdinfgjgfjir&params{categories[cat]["param"]}&categoryId={cat}&locationId={location_id}&page={page+1}&priceAsc&priceMin={curent_min_price}&lastStamp=1660769220&display=list&limit=50',
                                                headers=headers)
                        print('check2 url: ', f'https://m.avito.ru/api/11/items?key=af0deccbgcgidddjgnvljitntccdduijhdinfgjgfjir&params{categories[cat]["param"]}&categoryId={cat}&locationId={location_id}&page={page+1}&priceAsc&priceMin={curent_min_price}&lastStamp=1660769220&display=list&limit=50')
                        check2_json = json.loads(replacer(check2.text))
                        # if int(check2_json['result']['mainCount']) > 0:
                        if check2_json['result']['items']:
                            flag2 = True
                            page += 1
                            print('check2: ', flag2)
                        else:
                            flag2 = False
                            print('check2: ', flag2)

                    check = s.request('GET',
                                      f'https://m.avito.ru/api/11/items?key=af0deccbgcgidddjgnvljitntccdduijhdinfgjgfjir&params{categories[cat]["param"]}&categoryId={cat}&locationId={location_id}&page=1&priceAsc&priceMin={min_price}&lastStamp=1660769220&display=list&limit=50',
                                      headers=headers)
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

            print(f'Прогон по городу {city_rus} завершен\n'
                  f'Всего добавлено {len(orders)}')

            with open(f'items-{loc}.json', 'w') as file2:
                json.dump(orders, file2, indent=4, ensure_ascii=False)

            if os.path.exists('items-all-old2.json'):
                with open('items-all-old2.json', 'r', encoding='utf-8') as file_all:
                    all_orders = json.load(file_all)
                    all_orders.update(orders)
                print(f'Всего в общем файле: {len(all_orders)}')
                with open('items-all-old2.json', 'w', encoding='utf-8') as file_all:
                    json.dump(all_orders, file_all, indent=4, ensure_ascii=False)
                    print('Готово')
            else:
                print('Создаем файл')
                with open('items-all-old2.json', 'w', encoding='utf-8') as file_all:
                    json.dump(orders, file_all, indent=4, ensure_ascii=False)

        except Exception:
            print('Ошибка при проходе по каталогу объявлений ', Exception)
            count_except_city += 1
            time.sleep(rd.randint(20, 30))

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


if __name__ == '__main__':
    start_parse()


# ['Майкоп',
#  'Уфа',
#  'Улан-Удэ',
#  'Махачкала',
#  'Магас',
#  'Нальчик',
#  'Элиста',
#  'Петрозаводск',
#  'Сыктывкар',
#  'Симферополь',
#  'Йошкар-Ола',
#  'Саранск',
#  'Якутск',
#  'Владикавказ',
#  'Казань',
#  'Кызыл',
#  'Ижевск',
#  'Абакан',
#  'Грозный',
#  'Чебоксары',
#  'Барнаул',
#  'Чита',
#  'Петропавловск-Камчатский',
#  'Краснодар',
#  'Красноярск',
#  'Пермь',
#  'Владивосток',
#  'Ставрополь',
#  'Хабаровск',
#  'Благовещенск',
#  'Архангельск',
#  'Астрахань',
#  'Белгород',
#  'Брянск',
#  'Владимир',
#  'Волгоград',
#  'Вологда',
#  'Воронеж',
#  'Иваново',
#  'Иркутск',
#  'Калининград',
#  'Калуга',
#  'Кемерово',
#  'Киров',
#  'Кострома',
#  'Курган',
#  'Курск',
#  'Липецк',
#  'Магадан',
#  'Красногорск',
#  'Нижний Новгород',
#  'Великий Новгород',
#  'Новосибирск',
#  'Омск',
#  'Оренбург',
#  'Орёл',
#  'Пенза',
#  'Псков',
#  'Ростов-на-Дону',
#  'Рязань',
#  'Самара',
#  'Саратов',
#  'Южно-Сахалинск',
#  'Екатеринбург',
#  'Смоленск',
#  'Тамбов',
#  'Тверь',
#  'Томск',
#  'Тула',
#  'Тюмень',
#  'Ульяновск',
#  'Челябинск',
#  'Ярославль',
#  'Москва',
#  'Санкт-Петербург',
#  'Севастополь',
#  'Биробиджан',
#  'Нарьян-Мар',
#  'Ханты-Мансийск',
#  'Анадырь',
#  'Салехард']