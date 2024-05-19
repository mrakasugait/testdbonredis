# https://www.youtube.com/watch?v=jP7p2okKdJA
from flask import Flask
import os
import sys
import datetime
import pandas as pd
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, MetaData, Table, select, func, desc, between, Integer, cast, and_
from flask import Flask,  render_template, redirect, request, url_for
jo = ['桐生', '戸田', '江戸川', '平和島', '多摩川', '浜名湖', '蒲郡', '常滑', '津', '三国', 'びわこ',
      '住之江', '尼崎', '鳴門', '丸亀', '児島', '宮島', '徳山', '下関', '若松', '芦屋', '福岡', '唐津', '大村']

# url_forはCSS読み込み用
# from flask_sqlalchemy import SQLAlchemy

engine = create_engine('sqlite:///tmp03.db')
Session = sessionmaker(bind=engine)
session = Session()
metadata = MetaData()

uriage = Table('uriage', metadata, autoload_replace=True, autoload_with=engine)
hatsubai = Table('hatsubai', metadata,
                 autoload_replace=True, autoload_with=engine)

# app = Flask(__name__)

if getattr(sys, 'frozen', False):
    template_folder = os.path.join(sys._MEIPASS, 'templates')
    app = Flask(__name__, template_folder=template_folder)
else:
    app = Flask(__name__)


def execNo1(request):
    syukeijo = request.form['syukeijo']
    year = request.form['year']
    month = request.form['month']
    grade = request.form['grade']
    kubun = request.form['kubun']
    syukei = request.form['syukei']
    series = request.form['series']
    sort = request.form['sort']

    if month == '':
        s_period = f'{year}0401'
        e_period = f'{str(int(year)+1)}0331'
    elif month in ['01', '02', '03']:
        s_period = f'{str(int(year)+1)}{month}01'
        e_period = f'{str(int(year)+1)}{month}31'
    else:
        s_period = f'{year}{month}01'
        e_period = f'{year}{month}31'

    def func_syukei(query, syukeijo, syukei, sort):
        if syukeijo == 'jo':
            if syukei == 'avg':
                query = query.filter(uriage.columns.総売上 > 0).group_by('競走場').add_columns(
                    cast(func.avg(uriage.columns.総売上).label('new'), Integer))
            elif syukei == 'sum':
                query = query.group_by('競走場').add_columns(
                    cast(func.sum(uriage.columns.総売上).label('new'), Integer))
        else:
            if syukei == 'avg':
                query = query.filter(uriage.columns.総売上 > 0).group_by('開催施行者').add_columns(
                    cast(func.avg(uriage.columns.総売上).label('new'), Integer))
            elif syukei == 'sum':
                query = query.group_by('開催施行者').add_columns(
                    cast(func.sum(uriage.columns.総売上).label('new'), Integer))

        if syukeijo == 'jo':
            if sort == 'desc':
                return query.order_by(desc('new'))
            elif sort == 'asc':
                return query.order_by('new')
            else:
                return query.order_by('競走場')
        else:
            if sort == 'desc':
                return query.order_by(desc('new'))
            elif sort == 'asc':
                return query.order_by('new')
            else:
                return query

    if syukeijo == 'jo':
        if grade != '':
            if kubun != '':
                if series != '':
                    query = session.query(uriage.columns.競走場).filter(between(uriage.columns.開催日付, s_period, e_period),
                                                                     uriage.columns.開催区分 == kubun, uriage.columns.グレード == grade, uriage.columns.シリーズ戦等 == series)
                else:
                    query = session.query(uriage.columns.競走場).filter(between(uriage.columns.開催日付, s_period, e_period),
                                                                     uriage.columns.開催区分 == kubun, uriage.columns.グレード == grade)
            else:
                if series != '':
                    query = session.query(uriage.columns.競走場).filter(between(uriage.columns.開催日付, s_period, e_period),
                                                                     uriage.columns.グレード == grade, uriage.columns.シリーズ戦等 == series)
                else:
                    query = session.query(uriage.columns.競走場).filter(between(uriage.columns.開催日付, s_period, e_period),
                                                                     uriage.columns.グレード == grade,)
        else:
            if kubun != '':
                if series != '':
                    query = session.query(uriage.columns.競走場).filter(between(uriage.columns.開催日付, s_period, e_period),
                                                                     uriage.columns.開催区分 == kubun, uriage.columns.シリーズ戦等 == series)
                else:
                    query = session.query(uriage.columns.競走場).filter(between(uriage.columns.開催日付, s_period, e_period),
                                                                     uriage.columns.開催区分 == kubun)
            else:
                if series != '':
                    query = session.query(uriage.columns.競走場).filter(between(uriage.columns.開催日付, s_period, e_period),
                                                                     uriage.columns.シリーズ戦等 == series)
                else:
                    query = session.query(uriage.columns.競走場).filter(
                        between(uriage.columns.開催日付, s_period, e_period))
    else:
        if grade != '':
            if kubun != '':
                if series != '':
                    query = session.query(uriage.columns.開催施行者).filter(between(uriage.columns.開催日付,
                                                                               s_period, e_period), uriage.columns.開催区分 == kubun,
                                                                       uriage.columns.グレード == grade, uriage.columns.シリーズ戦等 == series)
                else:
                    query = session.query(uriage.columns.開催施行者).filter(between(uriage.columns.開催日付,
                                                                               s_period, e_period), uriage.columns.開催区分 == kubun, uriage.columns.グレード == grade)
            else:
                if series != '':
                    query = session.query(uriage.columns.開催施行者).filter(between(uriage.columns.開催日付,
                                                                               s_period, e_period), uriage.columns.グレード == grade, uriage.columns.シリーズ戦等 == series)
                else:
                    query = session.query(uriage.columns.開催施行者).filter(between(uriage.columns.開催日付,
                                                                               s_period, e_period), uriage.columns.グレード == grade)
        else:
            if kubun != '':
                if series != '':
                    query = session.query(uriage.columns.開催施行者).filter(between(uriage.columns.開催日付, s_period,
                                                                               e_period), uriage.columns.開催区分 == kubun, uriage.columns.シリーズ戦等 == series)
                else:
                    query = session.query(uriage.columns.開催施行者).filter(between(uriage.columns.開催日付, s_period,
                                                                               e_period), uriage.columns.開催区分 == kubun)
            else:
                if series != '':
                    query = session.query(uriage.columns.開催施行者).filter(between(uriage.columns.開催日付,
                                                                               s_period, e_period), uriage.columns.シリーズ戦等 == series)
                else:
                    query = session.query(uriage.columns.開催施行者).filter(
                        between(uriage.columns.開催日付, s_period, e_period))

    query = func_syukei(query, syukeijo, syukei, sort)
    return query, 'no1'


def execNo2(request):
    jo = request.form['jo']
    datestr = request.form['datestr']
    std_day = datetime.datetime.strptime(datestr, '%Y%m%d')

    if jo != '' and datestr != '':
        query = session.query(uriage).filter(
            uriage.columns.開催日付 == datestr, uriage.columns.競走場 == jo)
        query = session.query(uriage).filter(between(uriage.columns.開催日付,
                                                     (std_day-datetime.timedelta(days=10)
                                                      ).strftime('%Y%m%d'),
                                                     (std_day+datetime.timedelta(days=10)).strftime('%Y%m%d')),
                                             uriage.columns.競走名称 == query[0][7], uriage.columns.競走場 == jo)
        return query, 'no2'


def execNo3(request):
    title = request.form['title']

    if title != '':
        """
        query = session.query(uriage).filter(
            uriage.columns.競走名称.contains(title), (uriage.columns.シリーズ戦等 == 'SG') | (uriage.columns.シリーズ戦等 == '全国発売G1') | (uriage.columns.シリーズ戦等 == 'G2') | (uriage.columns.シリーズ戦等 == '3Days'))
        for i in query:
            print(i)
        return query, 'no3'
        """
        query = session.query(uriage).filter(
            uriage.columns.競走名称.contains(title), (uriage.columns.シリーズ戦等 == 'SG') | (uriage.columns.シリーズ戦等 == '全国発売G1') | (uriage.columns.シリーズ戦等 == 'G2') | (uriage.columns.シリーズ戦等 == '3Days'))

        query2 = session.query(hatsubai.columns.売上金額).join(uriage, and_(uriage.columns.開催日付 == hatsubai.columns.開催日付, uriage.columns.競走場 == hatsubai.columns.競走場)).filter(uriage.columns.競走名称.contains(title),
                                                                                                                                                                          (uriage.columns.シリーズ戦等 == 'SG') | (uriage.columns.シリーズ戦等 == '全国発売G1') | (
                                                                                                                                                                              uriage.columns.シリーズ戦等 == 'G2') | (uriage.columns.シリーズ戦等 == '3Days'),
                                                                                                                                                                          hatsubai.columns.発売場.contains(hatsubai.columns.競走場))

        query3 = session.query(hatsubai.columns.売上金額).join(uriage, and_(uriage.columns.開催日付 == hatsubai.columns.開催日付, uriage.columns.競走場 == hatsubai.columns.競走場)).filter(uriage.columns.競走名称.contains(title),
                                                                                                                                                                          (uriage.columns.シリーズ戦等 == 'SG') | (uriage.columns.シリーズ戦等 == '全国発売G1') | (
                                                                                                                                                                              uriage.columns.シリーズ戦等 == 'G2') | (uriage.columns.シリーズ戦等 == '3Days'),
                                                                                                                                                                          hatsubai.columns.発売場 == '025:電話投票計')
        # クエリをリストに統合[0:日付,1:競走場,2:施行者,3:グレード,4:開催区分,5:競走名,6:総売上,7:本場売上,8:電話売上]
        datalist = []
        for i, j, k in zip(list(query), list(query2), list(query3)):
            data = []
            data.append(i[0])
            data.append(i[1])
            data.append(i[2])
            data.append(i[5])
            data.append(i[6])
            data.append(i[7])
            data.append(i[9])
            data.append(j[0])
            data.append(k[0])
            datalist.append(data)

        return datalist, 'no3'


def execNo4(request):
    year = request.form['year2']
    series = request.form['series2']
    kubun = request.form['kubun2']
    print('-----no4')
    if kubun != '':
        query = session.query(uriage).filter(uriage.columns.シリーズ戦等 == series, between(
            uriage.columns.開催日付, f'{year}0401', f'{str(int(year)+1)}0331'), uriage.columns.開催区分 == kubun)
        query = query.order_by(uriage.columns.競走場)
        return query, 'no4'
    else:
        query = session.query(uriage).filter(uriage.columns.シリーズ戦等 == series, between(
            uriage.columns.開催日付, f'{year}0401', f'{str(int(year)+1)}0331'))
        query = query.order_by(uriage.columns.競走場)
        return query, 'no4'


def execNo5(request):
    # 0:開催日付,1:発売場,2:競走場,3:開催施行者,4:発売金額,5:返還金額,6:売上金額,7:払戻予定
    year5_1 = request.form['year5_1']
    year5_2 = request.form['year5_2']
    grade5 = request.form['grade5']
    kubun5 = request.form['kubun5']
    print('-----no5')
    if kubun5 != '':
        if grade5 != '':
            query = session.query(hatsubai).join(uriage, and_(uriage.columns.開催日付 == hatsubai.columns.開催日付, uriage.columns.競走場 == hatsubai.columns.競走場)).filter(between(
                uriage.columns.開催日付, f'{year5_1}0401', f'{str(int(year5_2)+1)}0331'), ~(hatsubai.columns.発売場.contains(hatsubai.columns.競走場)), hatsubai.columns.売上金額 > 0, uriage.columns.開催区分 == kubun5, uriage.columns.グレード == grade5)
        else:
            query = session.query(hatsubai).join(uriage, and_(uriage.columns.開催日付 == hatsubai.columns.開催日付, uriage.columns.競走場 == hatsubai.columns.競走場)).filter(between(
                uriage.columns.開催日付, f'{year5_1}0401', f'{str(int(year5_2)+1)}0331'), ~(hatsubai.columns.発売場.contains(hatsubai.columns.競走場)), hatsubai.columns.売上金額 > 0, uriage.columns.開催区分 == kubun5)
    else:
        if grade5 != '':
            query = session.query(hatsubai).join(uriage, and_(uriage.columns.開催日付 == hatsubai.columns.開催日付, uriage.columns.競走場 == hatsubai.columns.競走場)).filter(between(
                uriage.columns.開催日付, f'{year5_1}0401', f'{str(int(year5_2)+1)}0331'), ~(hatsubai.columns.発売場.contains(hatsubai.columns.競走場)), hatsubai.columns.売上金額 > 0, uriage.columns.グレード == grade5)
        else:
            query = session.query(hatsubai).join(uriage, and_(uriage.columns.開催日付 == hatsubai.columns.開催日付, uriage.columns.競走場 == hatsubai.columns.競走場)).filter(between(
                uriage.columns.開催日付, f'{year5_1}0401', f'{str(int(year5_2)+1)}0331'), ~(hatsubai.columns.発売場.contains(hatsubai.columns.競走場)), hatsubai.columns.売上金額 > 0)

    query = query.group_by('発売場').add_columns(
        cast(func.avg(hatsubai.columns.売上金額).label('new'), Integer))
    query = query.order_by(hatsubai.columns.発売場)
    return query, 'no5', f'{year5_1}0401', f'{str(int(year5_2)+1)}0331', grade5, kubun5


def execNo6(request, kubun):
    m_period = [['20190401', '20190430'], ['20190501', '20190531'], ['20190601', '20190630'], ['20190701', '20190731'], ['20190801', '20190831'], ['20190901', '20190930'], ['20191001', '20191031'], ['20191101', '20191130'], ['20191201', '20191231'], ['20200101', '20200131'], ['20200201', '20200229'], ['20200301', '20200331'],
                ['20200401', '20200430'], ['20200501', '20200531'], ['20200601', '20200630'], ['20200701', '20200731'], ['20200801', '20200831'], ['20200901', '20200930'], [
                    '20201001', '20201031'], ['20201101', '20221130'], ['20201201', '20201231'], ['20210101', '20210131'], ['20210201', '20210228'], ['20210301', '20210331'],
                ['20210401', '20210430'], ['20210501', '20210531'], ['20210601', '20210630'], ['20210701', '20210731'], ['20210801', '20210831'], ['20210901', '20210930'], [
                    '20211001', '20211031'], ['20211101', '20211130'], ['20211201', '20211231'], ['20220101', '20220131'], ['20220201', '20220228'], ['20220301', '20220331'],
                ['20220401', '20220430'], ['20220501', '20220531'], ['20220601', '20220630'], ['20220701', '20220731'], ['20220801', '20220831'], ['20220901', '20220930'], [
                    '20221001', '20221031'], ['20221101', '20221130'], ['20221201', '20221231'], ['20230101', '20230131'], ['20230201', '20230228'], ['20230301', '20230331'],
                ['20230401', '20230430'], ['20230501', '20230531'], ['20230601', '20230630'], ['20230701', '20230731'], ['20230801', '20230831'], ['20230901', '20230930'], [
                    '20231001', '20231031'], ['20231101', '20231130'], ['20231201', '20231231'], ['20240101', '20240131'], ['20240201', '20240229'], ['20240301', '20240331'],
                ['20240401', '20240430'], ['20240501', '20240531'], ['20240601', '20240630'], ['20240701', '20240731'], ['20240801', '20240831'], ['20240901', '20240930'], ['20241001', '20241031'], ['20241101', '20241130'], ['20241201', '20241231'], ['20250101', '20250131'], ['20250201', '20250228'], ['20250301', '20250331']]
    q_period = [['20190401', '20190630'], ['20190701', '20190930'], ['20191001', '20191231'], ['20200101', '20200331'], ['20200401', '20200630'], ['20200701', '20200930'], ['20201001', '20201231'], ['20210101', '20210331'],
                ['20210401', '20210630'], ['20210701', '20210930'], ['20211001', '20211231'], ['20220101', '20220331'], ['20220401', '20220630'], ['20220701', '20220930'], [
                    '20221001', '20221231'], ['20230101', '20230331'], ['20230401', '20230630'], ['20230701', '20230930'], ['20231001', '20231231'], ['20240101', '20240331'],
                ['20240401', '20240630'], ['20240701', '20240930'], ['20241001', '20241231'], ['20250101', '20250331']]
    y_period = [['20190401', '20200331'], ['20200401', '20210331'], ['20210401', '20220331'], [
        '20220401', '20230331'], ['20230401', '20240331'], ['20240401', '20250331']]

    grade = []
    queryList = []
    print(type(list(request.form)))

    # if 'SG' in request.form['flexCheckSG']:
    if 'SG' in request.form.values():
        grade.append('SG')

    if 'G1' in request.form.values():
        grade.append('G1')

    if 'G2' in request.form.values():
        grade.append('G2')

    if 'G3' in request.form.values():
        grade.append('G3')

    if '一般' in request.form.values():
        grade.append('一般')
    print(grade)

    if request.form['flexRadiosyukei'] == 'month':
        for i in range(len(m_period)):
            if 'その他一般' in request.form.values():
                if kubun != 'D&S' and kubun != 'N&M':
                    if 'zentai' in request.form.values():
                        query = session.query(uriage.columns.競走場).filter(between(uriage.columns.開催日付, m_period[i][0], m_period[i][1]),
                                                                         uriage.columns.開催区分 == kubun, uriage.columns.シリーズ戦等.in_(['それ以外G3', '一般競走']))
                    else:
                        query = session.query(hatsubai.columns.競走場).join(uriage, and_(uriage.columns.開催日付 == hatsubai.columns.開催日付, uriage.columns.競走場 == hatsubai.columns.競走場)).filter(between(
                            hatsubai.columns.開催日付, m_period[i][0], m_period[i][1]), hatsubai.columns.売上金額 > 0, uriage.columns.開催区分 == kubun, uriage.columns.シリーズ戦等.in_(['それ以外G3', '一般競走']), hatsubai.columns.発売場 == '025:電話投票計')
                elif kubun == 'D&S':
                    if 'zentai' in request.form.values():
                        query = session.query(uriage.columns.競走場).filter(between(uriage.columns.開催日付, m_period[i][0], m_period[i][1]),
                                                                         uriage.columns.開催区分.in_(['D', 'S']), uriage.columns.シリーズ戦等.in_(['それ以外G3', '一般競走']))
                    else:
                        query = session.query(hatsubai.columns.競走場).join(uriage, and_(uriage.columns.開催日付 == hatsubai.columns.開催日付, uriage.columns.競走場 == hatsubai.columns.競走場)).filter(between(
                            hatsubai.columns.開催日付, m_period[i][0], m_period[i][1]), hatsubai.columns.売上金額 > 0, uriage.columns.開催区分.in_(['D', 'S']), uriage.columns.シリーズ戦等.in_(['それ以外G3', '一般競走']), hatsubai.columns.発売場 == '025:電話投票計')
                elif kubun == 'N&M':
                    if 'zentai' in request.form.values():
                        query = session.query(uriage.columns.競走場).filter(between(uriage.columns.開催日付, m_period[i][0], m_period[i][1]),
                                                                         uriage.columns.開催区分.in_(['N', 'MN']), uriage.columns.シリーズ戦等.in_(['それ以外G3', '一般競走']))
                    else:
                        query = session.query(hatsubai.columns.競走場).join(uriage, and_(uriage.columns.開催日付 == hatsubai.columns.開催日付, uriage.columns.競走場 == hatsubai.columns.競走場)).filter(between(
                            hatsubai.columns.開催日付, m_period[i][0], m_period[i][1]), hatsubai.columns.売上金額 > 0, uriage.columns.開催区分.in_(['N', 'MN']), uriage.columns.シリーズ戦等.in_(['それ以外G3', '一般競走']), hatsubai.columns.発売場 == '025:電話投票計')

            else:
                if kubun != 'D&S' and kubun != 'N&M':
                    if 'zentai' in request.form.values():
                        query = session.query(uriage.columns.競走場).filter(between(uriage.columns.開催日付, m_period[i][0], m_period[i][1]),
                                                                         uriage.columns.開催区分 == kubun, uriage.columns.グレード.in_(grade))
                    else:
                        query = session.query(hatsubai.columns.競走場).join(uriage, and_(uriage.columns.開催日付 == hatsubai.columns.開催日付, uriage.columns.競走場 == hatsubai.columns.競走場)).filter(between(
                            hatsubai.columns.開催日付, m_period[i][0], m_period[i][1]), hatsubai.columns.売上金額 > 0, uriage.columns.開催区分 == kubun, uriage.columns.グレード.in_(grade), hatsubai.columns.発売場 == '025:電話投票計')
                elif kubun == 'D&S':
                    if 'zentai' in request.form.values():
                        query = session.query(uriage.columns.競走場).filter(between(uriage.columns.開催日付, m_period[i][0], m_period[i][1]),
                                                                         uriage.columns.開催区分.in_(['D', 'S']), uriage.columns.グレード.in_(grade))
                    else:
                        query = session.query(hatsubai.columns.競走場).join(uriage, and_(uriage.columns.開催日付 == hatsubai.columns.開催日付, uriage.columns.競走場 == hatsubai.columns.競走場)).filter(between(
                            hatsubai.columns.開催日付, m_period[i][0], m_period[i][1]), hatsubai.columns.売上金額 > 0, uriage.columns.開催区分.in_(['D', 'S']), uriage.columns.グレード.in_(grade), hatsubai.columns.発売場 == '025:電話投票計')
                elif kubun == 'N&M':
                    if 'zentai' in request.form.values():
                        query = session.query(uriage.columns.競走場).filter(between(uriage.columns.開催日付, m_period[i][0], m_period[i][1]),
                                                                         uriage.columns.開催区分.in_(['N', 'MN']), uriage.columns.グレード.in_(grade))
                    else:
                        query = session.query(hatsubai.columns.競走場).join(uriage, and_(uriage.columns.開催日付 == hatsubai.columns.開催日付, uriage.columns.競走場 == hatsubai.columns.競走場)).filter(between(
                            hatsubai.columns.開催日付, m_period[i][0], m_period[i][1]), hatsubai.columns.売上金額 > 0, uriage.columns.開催区分.in_(['N', 'MN']), uriage.columns.グレード.in_(grade), hatsubai.columns.発売場 == '025:電話投票計')
            if 'zentai' in request.form.values():
                query = query.filter(uriage.columns.総売上 > 0).group_by('競走場').add_columns(
                    cast(func.avg(uriage.columns.総売上).label('new'), Integer))
            else:
                query = query.filter(hatsubai.columns.売上金額 > 0).group_by('競走場').add_columns(
                    cast(func.avg(hatsubai.columns.売上金額).label('new'), Integer))

            queryList.append(query)
        return queryList, m_period

    elif request.form['flexRadiosyukei'] == 'quater':
        for i in range(len(q_period)):
            if 'その他一般' in request.form.values():
                if kubun != 'D&S' and kubun != 'N&M':
                    query = session.query(uriage.columns.競走場).filter(between(uriage.columns.開催日付, q_period[i][0], q_period[i][1]),
                                                                     uriage.columns.開催区分 == kubun, uriage.columns.シリーズ戦等.in_(['それ以外G3', '一般競走']))
                elif kubun == 'D&S':
                    query = session.query(uriage.columns.競走場).filter(between(uriage.columns.開催日付, q_period[i][0], q_period[i][1]),
                                                                     uriage.columns.開催区分.in_(['D', 'S']), uriage.columns.シリーズ戦等.in_(['それ以外G3', '一般競走']))
                elif kubun == 'N&M':
                    query = session.query(uriage.columns.競走場).filter(between(uriage.columns.開催日付, q_period[i][0], q_period[i][1]),
                                                                     uriage.columns.開催区分.in_(['N', 'MN']), uriage.columns.シリーズ戦等.in_(['それ以外G3', '一般競走']))
            else:
                if kubun != 'D&S' and kubun != 'N&M':
                    query = session.query(uriage.columns.競走場).filter(between(uriage.columns.開催日付, q_period[i][0], q_period[i][1]),
                                                                     uriage.columns.開催区分 == kubun, uriage.columns.グレード.in_(grade))
                elif kubun == 'D&S':
                    query = session.query(uriage.columns.競走場).filter(between(uriage.columns.開催日付, q_period[i][0], q_period[i][1]),
                                                                     uriage.columns.開催区分.in_(['D', 'S']), uriage.columns.グレード.in_(grade))
                elif kubun == 'N&M':
                    query = session.query(uriage.columns.競走場).filter(between(uriage.columns.開催日付, q_period[i][0], q_period[i][1]),
                                                                     uriage.columns.開催区分.in_(['N', 'MN']), uriage.columns.グレード.in_(grade))

            query = query.filter(uriage.columns.総売上 > 0).group_by('競走場').add_columns(
                cast(func.avg(uriage.columns.総売上).label('new'), Integer))
            queryList.append(query)
        return queryList, q_period

    elif request.form['flexRadiosyukei'] == 'year':
        for i in range(len(y_period)):
            if 'その他一般' in request.form.values():
                if kubun != 'D&S' and kubun != 'N&M':
                    if 'zentai' in request.form.values():
                        query = session.query(uriage.columns.競走場).filter(between(uriage.columns.開催日付, y_period[i][0], y_period[i][1]),
                                                                         uriage.columns.開催区分 == kubun, uriage.columns.シリーズ戦等.in_(['それ以外G3', '一般競走']))
                    else:
                        query = session.query(hatsubai.columns.競走場).join(uriage, and_(uriage.columns.開催日付 == hatsubai.columns.開催日付, uriage.columns.競走場 == hatsubai.columns.競走場)).filter(between(
                            hatsubai.columns.開催日付, y_period[i][0], y_period[i][1]), hatsubai.columns.売上金額 > 0, uriage.columns.開催区分 == kubun, uriage.columns.シリーズ戦等.in_(['それ以外G3', '一般競走']), hatsubai.columns.発売場 == '025:電話投票計')
                elif kubun == 'D&S':
                    if 'zentai' in request.form.values():
                        query = session.query(uriage.columns.競走場).filter(between(uriage.columns.開催日付, y_period[i][0], y_period[i][1]),
                                                                         uriage.columns.開催区分.in_(['D', 'S']), uriage.columns.シリーズ戦等.in_(['それ以外G3', '一般競走']))
                    else:
                        query = session.query(hatsubai.columns.競走場).join(uriage, and_(uriage.columns.開催日付 == hatsubai.columns.開催日付, uriage.columns.競走場 == hatsubai.columns.競走場)).filter(between(
                            hatsubai.columns.開催日付, y_period[i][0], y_period[i][1]), hatsubai.columns.売上金額 > 0, uriage.columns.開催区分.in_(['D', 'S']), uriage.columns.シリーズ戦等.in_(['それ以外G3', '一般競走']), hatsubai.columns.発売場 == '025:電話投票計')
                elif kubun == 'N&M':
                    if 'zentai' in request.form.values():
                        query = session.query(uriage.columns.競走場).filter(between(uriage.columns.開催日付, y_period[i][0], y_period[i][1]),
                                                                         uriage.columns.開催区分.in_(['N', 'MN']), uriage.columns.シリーズ戦等.in_(['それ以外G3', '一般競走']))
                    else:
                        query = session.query(hatsubai.columns.競走場).join(uriage, and_(uriage.columns.開催日付 == hatsubai.columns.開催日付, uriage.columns.競走場 == hatsubai.columns.競走場)).filter(between(
                            hatsubai.columns.開催日付, y_period[i][0], y_period[i][1]), hatsubai.columns.売上金額 > 0, uriage.columns.開催区分.in_(['N', 'MN']), uriage.columns.シリーズ戦等.in_(['それ以外G3', '一般競走']), hatsubai.columns.発売場 == '025:電話投票計')
            else:
                if kubun != 'D&S' and kubun != 'N&M':
                    if 'zentai' in request.form.values():
                        query = session.query(uriage.columns.競走場).filter(between(uriage.columns.開催日付, y_period[i][0], y_period[i][1]),
                                                                         uriage.columns.開催区分 == kubun, uriage.columns.グレード.in_(grade))
                    else:
                        query = session.query(hatsubai.columns.競走場).join(uriage, and_(uriage.columns.開催日付 == hatsubai.columns.開催日付, uriage.columns.競走場 == hatsubai.columns.競走場)).filter(between(
                            hatsubai.columns.開催日付, y_period[i][0], y_period[i][1]), hatsubai.columns.売上金額 > 0, uriage.columns.開催区分 == kubun, uriage.columns.グレード.in_(grade), hatsubai.columns.発売場 == '025:電話投票計')
                elif kubun == 'D&S':
                    if 'zentai' in request.form.values():
                        query = session.query(uriage.columns.競走場).filter(between(uriage.columns.開催日付, y_period[i][0], y_period[i][1]),
                                                                         uriage.columns.開催区分.in_(['D', 'S']), uriage.columns.グレード.in_(grade))
                    else:
                        query = session.query(hatsubai.columns.競走場).join(uriage, and_(uriage.columns.開催日付 == hatsubai.columns.開催日付, uriage.columns.競走場 == hatsubai.columns.競走場)).filter(between(
                            hatsubai.columns.開催日付, y_period[i][0], y_period[i][1]), hatsubai.columns.売上金額 > 0, uriage.columns.開催区分.in_(['D', 'S']), uriage.columns.グレード.in_(grade), hatsubai.columns.発売場 == '025:電話投票計')
                elif kubun == 'N&M':
                    if 'zentai' in request.form.values():
                        query = session.query(uriage.columns.競走場).filter(between(uriage.columns.開催日付, y_period[i][0], y_period[i][1]),
                                                                         uriage.columns.開催区分.in_(['N', 'MN']), uriage.columns.グレード.in_(grade))
                    else:
                        query = session.query(hatsubai.columns.競走場).join(uriage, and_(uriage.columns.開催日付 == hatsubai.columns.開催日付, uriage.columns.競走場 == hatsubai.columns.競走場)).filter(between(
                            hatsubai.columns.開催日付, y_period[i][0], y_period[i][1]), hatsubai.columns.売上金額 > 0, uriage.columns.開催区分.in_(['N', 'MN']), uriage.columns.グレード.in_(grade), hatsubai.columns.発売場 == '025:電話投票計')

            if 'zentai' in request.form.values():
                query = query.filter(uriage.columns.総売上 > 0).group_by('競走場').add_columns(
                    cast(func.avg(uriage.columns.総売上).label('new'), Integer))
            else:
                query = query.filter(hatsubai.columns.売上金額 > 0).group_by('競走場').add_columns(
                    cast(func.avg(hatsubai.columns.売上金額).label('new'), Integer))
            queryList.append(query)
        return queryList, y_period


def execNo7(request):
    # 0:開催日付,1:発売場,2:競走場,3:開催施行者,4:発売金額,5:返還金額,6:売上金額,7:払戻予定
    year7_1 = request.form['year7_1']
    year7_2 = request.form['year7_2']
    grade7 = request.form['grade7']
    kubun7 = request.form['kubun7']
    print(request.form)
    print('-----no7')

    if kubun7 != '':
        if grade7 != '':
            query = session.query(hatsubai).join(uriage, and_(uriage.columns.開催日付 == hatsubai.columns.開催日付, uriage.columns.競走場 == hatsubai.columns.競走場)).filter(between(
                uriage.columns.開催日付, f'{year7_1}0401', f'{str(int(year7_2)+1)}0331'), ~(hatsubai.columns.発売場.contains(hatsubai.columns.競走場)), hatsubai.columns.売上金額 > 0, uriage.columns.開催区分 == kubun7, uriage.columns.グレード == grade7)
        else:
            query = session.query(hatsubai).join(uriage, and_(uriage.columns.開催日付 == hatsubai.columns.開催日付, uriage.columns.競走場 == hatsubai.columns.競走場)).filter(between(
                uriage.columns.開催日付, f'{year7_1}0401', f'{str(int(year7_2)+1)}0331'), ~(hatsubai.columns.発売場.contains(hatsubai.columns.競走場)), hatsubai.columns.売上金額 > 0, uriage.columns.開催区分 == kubun7)
    else:
        if grade7 != '':
            query = session.query(hatsubai).join(uriage, and_(uriage.columns.開催日付 == hatsubai.columns.開催日付, uriage.columns.競走場 == hatsubai.columns.競走場)).filter(between(
                uriage.columns.開催日付, f'{year7_1}0401', f'{str(int(year7_2)+1)}0331'), ~(hatsubai.columns.発売場.contains(hatsubai.columns.競走場)), hatsubai.columns.売上金額 > 0, uriage.columns.グレード == grade7)
            print('AAA')
        else:
            query = session.query(hatsubai).join(uriage, and_(uriage.columns.開催日付 == hatsubai.columns.開催日付, uriage.columns.競走場 == hatsubai.columns.競走場)).filter(between(
                uriage.columns.開催日付, f'{year7_1}0401', f'{str(int(year7_2)+1)}0331'), ~(hatsubai.columns.発売場.contains(hatsubai.columns.競走場)), hatsubai.columns.売上金額 > 0)

    # query = query.order_by(hatsubai.columns.発売場)
    for i in query:
        print(i)
    return query, 'no7'


@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'GET':
        # return render_template('index.html', query=query)
        return render_template('index.html')
    else:
        # 複数ボタンがある場合にはname属性も送信される
        # print(request.form)
        if 'no1' in request.form:
            print('No1実行')
            try:
                query, no = execNo1(request)
                return render_template('index.html', query=query, no=no)
            finally:
                # return render_template('index.html')
                pass
        elif 'no2' in request.form:
            try:
                if request.form['jo'] != '' and request.form['datestr'] != '':
                    query, no = execNo2(request)
                else:
                    return render_template('index.html')
                if query != '':
                    print('No2実行')
                    return render_template('index.html', query=query, no=no)
                else:
                    return render_template('index.html')
            finally:
                pass
        elif 'no3' in request.form:
            try:
                if request.form['title'] != '':
                    datalist, no = execNo3(request)
                else:
                    return render_template('index.html')
                if datalist != '':
                    print('No3実行')
                    return render_template('index.html', datalist=datalist, no=no)
            finally:
                pass
        elif 'no4' in request.form:
            try:
                if request.form['series2'] != '':
                    query, no = execNo4(request)
                else:
                    return render_template('index.html')
                if query != '':
                    print('No4実行')
                    return render_template('index.html', query=query, no=no)
            finally:
                pass
        elif 'no5' in request.form:
            try:
                query, no, year5_1, year5_2, grade5, kubun5 = execNo5(request)
                print('No5実行')
                return render_template('index.html', query=query, no=no, year5_1=year5_1, year5_2=year5_2, grade5=grade5, kubun5=kubun5)
            finally:
                pass
        else:
            return render_template('index.html')


@app.route('/no6', methods=['GET', 'POST'])
def no6():
    morning = ['10:三国', '14:鳴門', '18:徳山', '21:芦屋', '23:唐津']
    day = ['02:戸田', '03:江戸川', '04:平和島', '05:多摩川', '06:浜名湖', '08:常滑',
           '09:津', '11:びわこ', '13:尼崎', '16:児島', '17:宮島', '22:福岡']
    summer = ['04:平和島', '05:多摩川', '06:浜名湖', '14:鳴門', '22:福岡']
    nighter = ['01:桐生', '07:蒲郡', '12:住之江', '15:丸亀', '19:下関', '20:若松', '24:大村']
    mid = ['19:下関', '20:若松', '24:大村']

    if request.method == 'GET':
        print('No6未実行')
        return render_template('no6.html')
    else:
        print(request.method)
        print(request.form)
        print('No6実行')

        if request.form['flexRadiokubun'] == 'N':
            queryList, period = execNo6(request, 'N')
            df = pd.DataFrame([["-" for _ in range(len(nighter))]
                              for _ in range(len(period))], columns=nighter)
            for i, query in enumerate(queryList):
                for row in query:
                    for j in range(len(nighter)):
                        if row[0] == nighter[j]:
                            df.iloc[i, j] = row[1]
                    """
                    if row[0] == '01:桐生':
                        df.iloc[i,0] = row[1]
                    elif row[0] == '07:蒲郡':
                        df.iloc[i,1] = row[1]
                    elif row[0] == '12:住之江':
                        df.iloc[i,2] = row[1]
                    elif row[0] == '15:丸亀':
                        df.iloc[i,3] = row[1]
                    elif row[0] == '19:下関':
                        df.iloc[i,4] = row[1]
                    elif row[0] == '20:若松':
                        df.iloc[i,5] = row[1]
                    elif row[0] == '24:大村':
                        df.iloc[i,6] = row[1]
                    """
        if request.form['flexRadiokubun'] == 'N&M':
            queryList, period = execNo6(request, 'N&M')
            df = pd.DataFrame([["-" for _ in range(len(nighter))]
                              for _ in range(len(period))], columns=nighter)
            for i, query in enumerate(queryList):
                for row in query:
                    for j in range(len(nighter)):
                        if row[0] == nighter[j]:
                            df.iloc[i, j] = row[1]

        elif request.form['flexRadiokubun'] == 'MN':
            queryList, period = execNo6(request, 'MN')
            df = pd.DataFrame([["-" for _ in range(len(mid))]
                              for _ in range(len(period))], columns=mid)
            for i, query in enumerate(queryList):
                for row in query:
                    for j in range(len(mid)):
                        if row[0] == mid[j]:
                            df.iloc[i, j] = row[1]

        elif request.form['flexRadiokubun'] == 'M':
            queryList, period = execNo6(request, 'M')
            # 空のデータフレームをつくる
            df = pd.DataFrame([["-" for _ in range(len(morning))]
                              for _ in range(len(period))], columns=morning)
            for i, query in enumerate(queryList):
                for row in query:
                    for j in range(len(morning)):
                        if row[0] == morning[j]:
                            df.iloc[i, j] = row[1]

        elif request.form['flexRadiokubun'] == 'D':
            queryList, period = execNo6(request, 'D')
            df = pd.DataFrame([["-" for _ in range(len(day))]
                              for _ in range(len(period))], columns=day)
            for i, query in enumerate(queryList):
                for row in query:
                    for j in range(len(day)):
                        if row[0] == day[j]:
                            df.iloc[i, j] = row[1]

        elif request.form['flexRadiokubun'] == 'S':
            queryList, period = execNo6(request, 'S')
            df = pd.DataFrame([["-" for _ in range(len(summer))]
                              for _ in range(len(period))], columns=summer)
            for i, query in enumerate(queryList):
                for row in query:
                    for j in range(len(summer)):
                        if row[0] == summer[j]:
                            df.iloc[i, j] = row[1]

        elif request.form['flexRadiokubun'] == 'D&S':
            queryList, period = execNo6(request, 'D&S')
            df = pd.DataFrame([["-" for _ in range(len(day))]
                              for _ in range(len(period))], columns=day)
            for i, query in enumerate(queryList):
                for row in query:
                    for j in range(len(day)):
                        if row[0] == day[j]:
                            df.iloc[i, j] = row[1]

        # print(df)
        return render_template('no6.html', df=df, period=period, request=request, no='no6')


@app.route('/no7', methods=['GET', 'POST'])
def no7():
    if request.method == 'GET':
        print('No7未実行')
        return render_template('no7.html')
    else:
        print('execNo7開始')
        query, no7 = execNo7(request)
        print('No7レンダリング開始')
        return render_template('no7.html', query=query, no7=no7)


if __name__ == '__main__':
    app.run(debug=True)
    # app.run(host="192.168.0.7", debug=False, port=80)
