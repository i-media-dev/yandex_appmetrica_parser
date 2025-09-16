import os

from parser.decorators import time_of_script
from parser.ya_appmetrica import AppmetricaSaveClient
from parser.utils import get_date_list


@time_of_script
def main():
    """Основная логика скрипта."""
    dates_list = get_date_list()
    token = str(os.getenv('YANDEX_APPMETRICA_TOKEN'))
    saver = AppmetricaSaveClient(token, dates_list)
    saver.save_data(
        shop_id='2550202',
        filename_temp='eapteka_direct.csv',
        filename_data='eapteka_appmetrica.csv'
    )


if __name__ == '__main__':
    main()
