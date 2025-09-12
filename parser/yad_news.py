import pandas as pd
import requests
import os
from os.path import abspath, dirname, join
from inspect import getsourcefile
from dotenv import load_dotenv
from datetime import datetime as dt, timedelta

load_dotenv()


def new_paths(new_name: str) -> str:
    path = dirname(abspath(str(getsourcefile(lambda: 0))))
    return join(path, new_name)


def get_data(date_reports: str, campaign_name: str) -> dict:
    date_start_month = '-'.join(date_reports.split('-')[:-1])
    date_start_month = f'{date_start_month}-01'
    date_obj = dt.strptime(date_reports, "%Y-%m-%d")
    seven_days_before = date_obj - timedelta(days=7)
    seven_days_before = seven_days_before.strftime("%Y-%m-%d")
    headers = {'Authorization': f'OAuth {os.getenv("APP_TOKEN")}'}
    url = f'https://api.appmetrica.yandex.ru/stat/v1/data?ids=2550202&date1={date_reports}&date2={date_reports}&group=Day&metrics=ym%3Aec2%3AecomRevenueFiatRUB%2Cym%3Aec2%3AecomOrdersCount&dimensions=ym%3Aec2%3Adate&limit=10000&filters=%28exists+ym%3Ao%3Adevice+with+%28exists%28urlParamKey%3D%3D%27utm_campaign%27+and+urlParamValue%3D%3D%27{
        campaign_name}%27%29+and+specialDefaultDate%3E%3D%27{seven_days_before}%27+and+specialDefaultDate%3C%3D%27{date_reports}%27%29%29&accuracy=1&include_undefined=true&currency=RUB&event_attribution=last_appmetrica&sort=-ym%3Aec2%3AecomOrdersCount&lang=ru&request_domain=ru'
    req = requests.get(url, headers=headers)
    # print(req.text)
    req = req.json()['data'][0]

    revenue, transactions = req['metrics']

    return [date_reports, campaign_name, int(float(transactions)), float(revenue)]


def add_ps(row):
    if 'srch' in row['CampaignName'] or '-srch-' in row['CampaignName']:
        return 'поиск'
    elif '-all-' in row['CampaignName']:
        return 'все'
    elif '-net-' in row['CampaignName'] or 'network' in row['CampaignName']:
        return 'сеть'
    else:
        return 'nd'


def add_type(row):
    # if 'ios' in row['CampaignName'] or 'android' in row['CampaignName']:
    #     return 'rmp'
    if 'cpm' in row['CampaignName']:
        return 'brandformance'
    elif '-brand' in row['CampaignName']:
        return 'brand'
    else:
        return 'nonbrand'


def add_apptype(row):
    if 'ios' in row['CampaignName']:
        return 'ios'
    elif 'android' in row['CampaignName']:
        return 'android'
    else:
        return 'web'


def geo(row):
    geo = row['CampaignName'].split('-')[0]
    return geo


def save_data(df_new: pd.DataFrame):

    print('\n\n[ + ]\tclean appmetrica_2.csv')
    # p = os.path.join(new_paths('data'), 'appmetrica_2.csv')
    p = os.path.join(path_capaign, 'appmetrica_2.csv')
    old_df = pd.read_csv(p, sep=';', encoding='cp1251', header=0)

    for dates in dates_list:
        old_df = pd.DataFrame(old_df[
            ~pd.Series(old_df['Date']).fillna('').str.contains(
                fr'{dates}', case=False, na=False)])

    print('[ + ]\tsave appmerica_2.csv')

    old_df = pd.concat([df_new, old_df])
    old_df.to_csv(p, index=False, header=True, sep=';', encoding='cp1251')
    # df_new.to_csv(p, index=False, header=True, sep=';', encoding='cp1251')
