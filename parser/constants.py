YANDEX_APPMETRICA_URL = 'https://api.appmetrica.yandex.ru/stat/v1/data'

DEFAULT_FOLDER = 'data'
"""Папка для сохранения .csv файлов по умолчанию."""

DAYS_TO_GENERATE = 1
"""Количество дней для генерации списка дат по умолчанию."""

DAYS_BEFORE = 7
"""Период в днях (7)."""

LIMIT = '1000'
"""Лимит выдачи отчета на страницу."""

CAMPAIGN_CATEGORIES = {
    'srch': 'поиск',
    '-all-': 'все',
    '-net-': 'сеть'
}

PLATFORM_TYPES = {
    'cpm': 'brandformance',
    '-brand': 'brand'
}

APP_TYPES = {
    'ios': 'ios',
    'android': 'android'
}

DEFAULT_RETURNES = {
    'campaign': 'nd',
    'platform': 'nonbrand',
    'app': 'web',
    'error': 'unknown'
}
