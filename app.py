from flask import Flask
app = Flask(__name__)
"""
@app.route('/')
def hello_world():
    return 'Hello World!!'
"""

import pandas as pd
import re
import requests
import datetime
from bs4 import BeautifulSoup

@app.route('/')
def hello_world():
    windms = 8
    joumei = {'桐': '桐 生', '戸': '戸 田', '江': '江 戸 川', '平': '平 和 島', '多': '多 摩 川', '浜': '浜 名 湖', '蒲': '蒲 郡', '常': '常 滑', '津': '津', '三': '三 国', 'び': 'び わ こ',
            '住': '住 之 江', '尼': '尼 崎', '鳴': '鳴 門', '丸': '丸 亀', '児': '児 島', '宮': '宮 島', '徳': '徳 山', '下': '下 関', '若': '若 松', '芦': '芦 屋', '福': '福 岡', '唐': '唐 津', '大': '大 村'}

    dt_now = datetime.datetime.now()
    # dt_now = datetime.datetime.now()+datetime.timedelta(hours=9)
    # dt_today = dt_now.day-1 if dt_now.hour < 9 else dt_now.day

    URL = 'http://www.motorboatracing-association.jp/index.html'

    response = requests.get(URL)
    response.encoding = response.apparent_encoding
    soup = BeautifulSoup(response.content, 'html.parser')
    print(soup.find('title').text)

    print('---------------------------------------------------------------------------')
    print(soup.find(class_="todaytime").text)
    element = soup.find(class_='top-kaisai kaisai')
    # print(element.text)
    li_joumei = element.text.split('\n')
    print(li_joumei)
    li_joumei02 = []
    for i in range(len(li_joumei)):
        # ここがポイント
        if not (li_joumei[i] == '－\r' or li_joumei[i] == '\r' or li_joumei[i] == ''):
            li_joumei02.append(li_joumei[i])
    print('---------------------------------------------------------------------------')
    print(len(li_joumei02), li_joumei02)

    kaisai_data = []
    tsusan_data = []
    for i in range(len(li_joumei02)):
        if i % 2 == 1:
            tsusan_data.append(li_joumei02[i])
        else:
            kaisai_data.append(li_joumei02[i].replace(' ', ''))
    if len(kaisai_data) > 25:
        print('-rが2個以上ある!')
        exit()

    for i in range(len(kaisai_data)):
        if kaisai_data[i] == '\r':
            print('hit!')
            kaisai_data.pop(i)
            tsusan_data.pop(i)
            break

    src_html = str(element)
    print(src_html)

    # ナイター・モーニング・ミッドナイト開催の抽出
    nighter = [m.start() for m in re.finditer('<img src="assets/img/icon_nighter.svg"/>', src_html)]
    nighterList = []
    for i in range(len(nighter)):
        startN = src_html.rfind('<span class="name">', 0,
                                nighter[i])+len('<span class="name">')
        nighterList.append(src_html[startN:startN+1])
    for i in range(len(nighterList)):
        nighterList[i] = joumei[nighterList[i]].replace(' ', '')

    morning = [m.start() for m in re.finditer(
        '<img src="assets/img/icon_morning.svg"/>', src_html)]
    morningList = []
    for i in range(len(morning)):
        startM = src_html.rfind('<span class="name">', 0,
                                morning[i])+len('<span class="name">')
        morningList.append(src_html[startM:startM+1])
    for i in range(len(morningList)):
        morningList[i] = joumei[morningList[i]].replace(' ', '')

    midnight = [m.start() for m in re.finditer(
        '<img src="assets/img/icon_midnight.svg"/>', src_html)]
    midnightList = []
    for i in range(len(midnight)):
        startM = src_html.rfind('<span class="name">', 0,
                                midnight[i])+len('<span class="name">')
        midnightList.append(src_html[startM:startM+1])
    for i in range(len(midnightList)):
        midnightList[i] = joumei[midnightList[i]].replace(' ', '')

    # 中止情報の抽出
    abort = [m.start() for m in re.finditer('中止', src_html)]
    abortList = []
    for i in range(len(abort)):
        startN = src_html.rfind('<span class="name">',
                                0, abort[i])+len('<span class="name">')
        abortList.append(src_html[startN:startN+1])
    for i in range(len(abortList)):
        abortList[i] = joumei[abortList[i]].replace(' ', '')

    print('---------------------------------------------------------------------------')
    print(morningList)
    print(nighterList)
    print(midnightList)
    print(abortList)
    df = pd.DataFrame()
    df['競走場'] = kaisai_data
    df['通算日'] = tsusan_data
    df['開催区分'] = ''

    print('---------------------------------------------------------------------------')
    print(df)

    for i in range(24):
        if df.iloc[i, 0] in morningList:
            df.iloc[i, 2] = 'M'
        elif df.iloc[i, 0] in nighterList:
            df.iloc[i, 2] = 'N'
        elif df.iloc[i, 0] in midnightList:
            df.iloc[i, 2] = 'MN'
        elif df.iloc[i, 0] != '':
            df.iloc[i, 2] = 'D'

    df = df[df['通算日'] != '－']
    df = df.reindex(columns=['競走場', '開催区分', '通算日'])
    df = df.reset_index(drop=True)
    print(df)
    df2 = pd.read_csv("loc.csv", encoding='shift-jis')
    df3 = df.merge(df2)

    df3['天気'] = ''
    df3['風速(m/s)'] = ''

    for i in range(len(df3)):
        url = "https://api.weatherapi.com/v1/forecast.json?key={key}&q={lat},{lon}&days=1&aqi=no&alerts=no&lang=ja"
        url = url.format(
            lat=df3.iloc[i, 3], lon=df3.iloc[i, 4], key="092ebedf037b41669ff144005230312")
        jsondata = requests.get(url).json()
        df3.iloc[i, 5] = jsondata["current"]["condition"]["text"]
        df3.iloc[i, 6] = round(jsondata["current"]["wind_kph"]/3.6, 1)

    df3.drop("緯度", axis=1, inplace=True)
    df3.drop("経度", axis=1, inplace=True)
    print('---------------------------------------------------------------------------')
    print(df3)
    print('---------------------------------------------------------------------------')
    print(dt_now)
    print('---------------------------------------------------------------------------')
    print(f'\n風速{windms}m/s以上の開催')
    print(df3[df3['風速(m/s)'] > windms])
    df3.drop("天気", axis=1, inplace=True)

    if dt_now.hour < 15:
        messages = df3[df3['風速(m/s)'] > windms].to_string()
        messages = '' if len(messages) ==0 else messages
    elif dt_now.hour < 17:
        messages = df3[(df3['風速(m/s)'] > windms) &
                    (df3['開催区分'].isin(['D', 'N', 'MN']))].to_string()
        messages = '' if len(messages) == 0 else messages
    elif dt_now.hour < 23:
        messages = df3[(df3['風速(m/s)'] > windms) & (df3['開催区分'].isin(['N', 'MN']))].to_string()
        messages = '' if len(messages) == 0 else messages
    else:
        messages = f'時間外もしくは風速{windms}m/s以上の開催はなし'
        messages = '' if len(messages) == 0 else messages

    return messages