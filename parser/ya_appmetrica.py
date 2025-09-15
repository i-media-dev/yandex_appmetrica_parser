import datetime as dt
import logging
from pathlib import Path

from dotenv import load_dotenv
import pandas as pd
import requests

from parser.constants import (
    APP_TYPES,
    CAMPAIGN_CATEGORIES,
    DAYS_BEFORE,
    DEFAULT_FOLDER,
    DEFAULT_RETURNES,
    LIMIT,
    PLATFORM_TYPES
)
from parser.logging_config import setup_logging

load_dotenv()
setup_logging()


class DataSaveClient:

    def __init__(
        self,
        token: str,
        dates_list: list,
        folder_name: str = DEFAULT_FOLDER,
        limit: str = LIMIT
    ):
        if not token:
            logging.error('Токен отсутствует или не действителен')
        self.token = token
        self.folder = folder_name
        self.dates_list = dates_list
        self.limit = limit or '1000'

    def _get_file_path(self, filename: str) -> Path:
        """Защищенный метод. Создает путь к файлу в указанной папке."""
        try:
            file_path = Path(__file__).parent.parent / self.folder
            file_path.mkdir(parents=True, exist_ok=True)
            return file_path / filename
        except Exception as e:
            logging.error(f'Ошибка: {e}')
            raise

    def _get_appmetrica_report(
        self,
        date_reports: str,
        campaign_name: str
    ) -> list:
        try:
            date_obj = dt.datetime.strptime(date_reports, '%Y-%m-%d')
            days_before = date_obj - dt.timedelta(days=DAYS_BEFORE)
            days_before = days_before.strftime('%Y-%m-%d')
            url = 'https://api.appmetrica.yandex.ru/stat/v1/data'
            headers = {
                "Authorization": f"OAuth {self.token}"}
            params = {
                "ids": "2550202",
                "date1": date_reports,
                "date2": date_reports,
                "group": "Day",
                "metrics": "ym:ec2:ecomRevenueFiatRUB,ym:ec2:ecomOrdersCount",
                "dimensions": "ym:ec2:date",
                "limit": self.limit,
                "accuracy": "1",
                "include_undefined": "true",
                "currency": "RUB",
                "event_attribution": "last_appmetrica",
                "sort": "-ym:ec2:ecomOrdersCount",
                "lang": "ru",
                "request_domain": "ru"
            }
            filters = (
                f"(exists ym:o:device with "
                f"(exists(urlParamKey=='utm_campaign' "
                f"and urlParamValue=='{campaign_name}') "
                f"and specialDefaultDate>='{days_before}' "
                f"and specialDefaultDate<='{date_reports}'))"
            )
            params['filters'] = filters

            response = requests.get(
                url,
                params=params,
                headers=headers,
                timeout=30
            )
            response.raise_for_status()
            data = response.json()

            if not data or 'data' not in data or not data['data']:
                logging.warning(
                    'Нет данных для кампании '
                    f'{campaign_name} на {date_reports}'
                )
                return [date_reports, campaign_name, 0, 0.0]

            first_data_item = data['data'][0]
            revenue, transactions = first_data_item.get('metrics', [0.0, 0.0])
            return [
                date_reports,
                campaign_name,
                int(float(transactions)),
                float(revenue)
            ]
        except requests.exceptions.RequestException as e:
            logging.error(f'Ошибка запроса к api: {e}')
            raise
        except Exception as e:
            logging.error(f'Ошибка: {e}')
            raise

    def _get_campaign_category(self, row) -> str:
        try:
            for tag, value in CAMPAIGN_CATEGORIES.items():
                if tag in row['CampaignName']:
                    return value
            return DEFAULT_RETURNES.get('campaign', '')
        except (AttributeError, IndexError, KeyError):
            return DEFAULT_RETURNES.get('error', '')

    def _get_platform_type(self, row) -> str:
        try:
            for tag, value in PLATFORM_TYPES.items():
                if tag in row['CampaignName']:
                    return value
            return DEFAULT_RETURNES.get('platform', '')
        except (AttributeError, IndexError, KeyError):
            return DEFAULT_RETURNES.get('error', '')

    def _get_app_type(self, row) -> str:
        try:
            for tag, value in APP_TYPES.items():
                if tag in row['CampaignName']:
                    return value
            return DEFAULT_RETURNES.get('app', '')
        except (AttributeError, IndexError, KeyError):
            return DEFAULT_RETURNES.get('error', '')

    def _get_geo(self, row):
        try:
            geo = row['CampaignName'].split('-')[0]
            return geo
        except (AttributeError, IndexError, KeyError):
            return DEFAULT_RETURNES.get('error', '')

    def get_filtered_cache_data(self, df_new: pd.DataFrame):
        try:
            temp_cache_path = self._get_file_path('appmetrica_2.csv')
            old_df = pd.read_csv(
                temp_cache_path,
                sep=';',
                encoding='cp1251',
                header=0
            )

            for dates in self.dates_list:
                old_df = pd.DataFrame(old_df[~pd.Series(
                    old_df['Date']
                ).fillna('').str.contains(
                    fr'{dates}',
                    case=False,
                    na=False
                )])

            old_df = pd.concat([df_new, old_df])
            old_df.to_csv(
                temp_cache_path,
                index=False,
                header=True,
                sep=';',
                encoding='cp1251'
            )
            return old_df
        except FileNotFoundError:
            logging.warning('Файл кэша не найден. Первый запуск.')
            return pd.DataFrame()
        except pd.errors.EmptyDataError:
            logging.warning('Файл кэша пустой.')
            return pd.DataFrame()
        except Exception as e:
            logging.error(f'Ошибка: {e}')
            raise

    def get_all_appmetrica_data(self) -> pd.DataFrame:
        df_new = pd.DataFrame(
            columns=['Date', 'CampaignName', 'transactions', 'revenue']
        )
        temp_cache_path = self._get_file_path('cashe_new.csv')
        try:
            campaign_df = pd.read_csv(
                temp_cache_path,
                sep=';', encoding='cp1251'
            )
            campaigns_list = campaign_df['CampaignName'].unique().tolist()
        except FileNotFoundError:
            logging.error('Файл с кампаниями не найден')
            return df_new

        for date_str in self.dates_list:
            for campaign_name in campaigns_list:
                try:
                    if 'rmp' in campaign_name:
                        continue

                    data = self._get_appmetrica_report(date_str, campaign_name)
                    df = pd.DataFrame(
                        [data],
                        columns=[
                            'Date',
                            'CampaignName',
                            'transactions',
                            'revenue'
                        ]
                    )
                    df_new = pd.concat([df_new, df])

                except Exception as e:
                    logging.error(
                        f'Ошибка для кампании {campaign_name} '
                        f'на дату {date_str}: {e}')
                    continue

        df_new['transactions'] = df_new['transactions'].astype(int)
        df_new['revenue'] = df_new['revenue'].astype(float)
        df_new['Device'] = 'mobile'
        df_new['sn'] = df_new.apply(self._get_campaign_category, axis=1)
        df_new['type'] = df_new.apply(self._get_platform_type, axis=1)
        df_new['apptype'] = 'web'
        df_new['geo'] = df_new.apply(self._get_geo, axis=1)

        return df_new

    def save_data(self, df_new, old_df) -> None:
        """Метод сохраняет новые данные, объединяя с существующими."""
        try:
            temp_cache_path = self._get_file_path('appmetrica_2.csv')
            if df_new.empty:
                logging.warning('Нет новых данных для сохранения')
                return
            if not isinstance(old_df, pd.DataFrame) or old_df.empty:
                df_new.to_csv(
                    temp_cache_path,
                    index=False,
                    header=True,
                    sep=';',
                    encoding='cp1251'
                )
                logging.info(
                    'Новые данные сохранены. Исторические данные отсутствовали'
                )
                return
            for dates in self.dates_list:
                old_df = old_df[~old_df['Date'].fillna('').str.contains(
                    fr'{dates}', case=False, na=False)]

            old_df = pd.concat([df_new, old_df])
            old_df.to_csv(
                temp_cache_path,
                index=False,
                header=True,
                sep=';',
                encoding='cp1251'
            )
            logging.info('Данные успешно обновлены')
        except Exception as e:
            logging.error(f'Ошибка во время обновления: {e}')
