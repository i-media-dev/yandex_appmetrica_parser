import os
import datetime as dt

import pandas as pd

from parser.yad_news import (
    get_data,
    add_ps,
    geo,
    add_type,
    save_data,
    new_paths
)


def main():
    c_list_temp = []
    c_list = []
    with open(path_capaign_cashe, 'r', encoding='cp1251') as f:
        for x, line in enumerate(f):
            if formatted_date in line and 'rmp' not in line:
                campaign_line = line.split(';')[1]
                if campaign_line not in c_list_temp:
                    c_list_temp.append(campaign_line)
                    c_list.append([campaign_line])

    df_new = pd.DataFrame(
        columns=['Date', 'CampaignName', 'transactions', 'revenue'])

    for dates in dates_list:
        print(dates)
        for campaign in c_list:
            try:
                data = get_data(dates, campaign[0])
                df = pd.DataFrame(
                    [data],
                    columns=['Date', 'CampaignName', 'transactions', 'revenue']
                )
                df_new = pd.concat([df_new, df])
            except Exception as err:
                continue
                # print(err)
    df_new['transactions'] = df_new['transactions'].astype(int)
    df_new['revenue'] = df_new['revenue'].astype(float)
    print(df_new.head())
    # df_new['Date'] = pd.to_datetime(df_new['Date'])
    df_new['Device'] = 'mobile'
    df_new['sn'] = df_new.apply(add_ps, axis=1)
    df_new['type'] = df_new.apply(add_type, axis=1)
    df_new['apptype'] = 'web'
    df_new['geo'] = df_new.apply(geo, axis=1)

    save_data(df_new)


if __name__ == '__main__':
    path_capaign = new_paths('')
    path_capaign = os.path.dirname(path_capaign)
    path_capaign = os.path.dirname(path_capaign)
    path_capaign = os.path.join(path_capaign, 'data')
    path_capaign_cashe = os.path.join(path_capaign, 'cashe_new.csv')

    yesterday = dt.datetime.now() - dt.timedelta(days=1)
    formatted_date = yesterday.strftime('%Y-%m-%d')
    tok = os.getenv('TOKEN_API')
    url_api = 'https://install.i-reports.ru/data'
    headers = {'Authorization': f'Bearer {tok}'}

    dates_list = []

    # for i in range(1, 10):
    for i in range(1, 2):
        tempday = dt.datetime.now()
        tempday -= dt.timedelta(days=i)
        tempday = tempday.strftime('%Y-%m-%d')
        dates_list.append(tempday)

    main()
