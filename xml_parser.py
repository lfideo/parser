from parser_body import XMLToGSpread #класс который был создан в parser_body
from ischedule import schedule, run_loop #модуль для автоматизации запуска скрипта 
from datetime import datetime

# ЯНДЕКС

# место расположения исходного файла
xml_source_url = "https://pics.capitalgroup.ru/pic/%D0%91%D0%BE%D0%B9/%D0%A0%D0%9A_%D0%AF%D0%BD%D0%B4%D0%B5%D0%BA%D1%81_%D0%A2%D1%80%D0%B8%D0%BA%D0%BE%D0%BB%D0%BE%D1%80.xml"
# название таблицы, в которой располагается таблица
ss_table_name = "Test table"
# Какие ключи необходимы для парсинга
keywords_parse = {
    "@internal-id": True,
    "sales-agent": "phone",
    "floor": True,
    "rooms": True,
    "area": {"value", "unit"},
    "living-space": {"value", "unit"},
    "price": {"value", "currency"}, # elif type(parse_info) == set
}

# pending_data = XMLToGSpread(xml_source_url, ss_table_name, keywords_parse, "ЯНДЕКС", ["Description", "Заголовок"])
# pending_data.send_to_spreads()

# ЦИАН

# место расположения исходного файла
xml_source_url = "https://pb13845.profitbase.ru/export/cian/0104041ea2653b9ac6e9ede4daa88ad3?scheme=https"
# название таблицы, в которой располагается таблица
ss_table_name = "Данные для фидов"
# Какие ключи необходимы для парсинга
keywords_parse_cian = {
    "JKSchema": [
        # пример нескольких значений под одним ключем
        {
            "content": {"Name", "Id"} #content - ключевое слово для парсинга вложенных запросов 
        },
        {
            "content": {"House>Id"}  # пример погружения через использования специального символа >
        }
    ],
    "Phones": {
        # пример нескольких значений под одним ключем
        "content": {"PhoneSchema>CountryCode", "PhoneSchema>Number"},
        "merge": {"name": "Phone", "items": ["CountryCode", "Number"]}
    },
    "ExternalId": True,
    "FloorNumber": True,
    "FlatRoomsCount": True,
    "TotalArea": True,
    "LivingArea": True,
    "BargainTerms": "Price",
}

pending_data = XMLToGSpread(xml_source_url, ss_table_name, keywords_parse_cian, "логос")
pending_data.send_to_spreads()

#print(datetime.now())

#schedule(pending_data.send_to_spreads, interval = 60000000.0)
#run_loop()









