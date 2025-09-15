import os

from parser.decorators import time_of_script
from parser.ya_appmetrica import DataSaveClient
from parser.utils import get_date_list


@time_of_script
def main():
    dates_list = get_date_list()
    token = os.getenv('YANDEX_APPMETRICA_TOKEN')
    saver = DataSaveClient(token, dates_list)
    new_df = saver.get_all_appmetrica_data()
    old_df = saver.get_filtered_cache_data(new_df)
    saver.save_data(new_df, old_df)


if __name__ == '__main__':
    main()
