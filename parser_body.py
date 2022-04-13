import gspread
import urllib3
import xmltodict
import json


class XMLToGSpread:
    def __init__(self, file_url, table_name, parse_keywords, worksheet_name="parsed_data", blank_headers=[]):
        self.header = []
        self.content = []
        self.offer_data = {}
        self.header_created = False
        self.table_name = table_name
        self.parse_keywords = parse_keywords
        self.worksheet_name = worksheet_name
        self.blank_headers = blank_headers

        if file_url.find("http") == -1: 
            xml_file = open(file_url, encoding="utf8")  # открываем для чтения исходный .xml файл с кодировкой utf8
            xml_tree = xml_file.read() #читаем наш файл
        else:
            http = urllib3.PoolManager()
            response = http.request("GET", file_url)  # создаем GET-запрос по ссылке file_url
            xml_tree = response.data  # записываем в xml_tree контент, полученный со страницы
        # преобразовываем XML-древо в словарь через xmltodict.parse()
        # далее конвертируем результат в JSON с помощью json.dumps()
        # также используем кодировку utf8, чтобы получить читабельный результат
        parsed_json = json.dumps(xmltodict.parse(xml_tree), ensure_ascii=False).encode("utf8")
        xml_dict = json.loads(parsed_json)  # преобразовываем JSON в словарь
        # берем первый ключ за корневой
        # (?) скорее всего его нужно напрямую писать, так как формат XML файлов всегда будет один
        root_key = ''
        for key in xml_dict:
            root_key = key

        root_dict = xml_dict[root_key]
        #print(root_dict)

        # пропускаем мусорные ключи, которые не нужны нам для парсинга
        # TODO: добавить список ключей, которые необходимо отбросить
        for key in root_dict:
            if key.find("@") != -1:
                continue
            if key == "generation-date":
                continue
            if key == "feed_version":
                continue
            self.sort_offers(root_dict[key])
            #print(root_dict[key])

    # функция, которая парсит словарь offers_list
    def sort_offers(self, offers_list):
        # сортируем всю информацию для дальнейшей отправки в google spreads
        for offer in offers_list:
            self.parse_offer(offer)

    # функция отправляет всю информацию, которую мы парсили в google spreadsheets
    def send_to_spreads(self):
        print("Sending data to Google Spreadsheets...")
        gc = gspread.service_account()
        sh = gc.open(self.table_name)

        worksheet_id = self.worksheet_name
        # сколько стобцов и рядов нам нужно для данной информации
        rows = len(self.header)
        cols = len(self.content)

        # получаем worksheet или создаем его, если он ещё не существует
        try:
            worksheet = sh.add_worksheet(title=worksheet_id, rows=rows, cols=cols)
        except gspread.exceptions.APIError:
            worksheet = sh.worksheet(worksheet_id)

        blank_headers_data = [] #вставляем сюда сохраненную информацию 

        # Удаляем header
        if len(worksheet.row_values(1)) > 0: #логика что в принципе header существует
            worksheet.delete_row(1)

            # сохраняем данные из столбцов с ручным вводом
            iterator = len(self.parse_keywords.keys()) + 1 #считает от конца +1 чтобы не удалять столбцы с ручным вводом  
            for k in range(iterator, iterator + len(self.blank_headers)):
                blank_headers_data.append(worksheet.col_values(k))

        # очищаем прежнюю информацию
        worksheet.clear()

        # создаем header
        worksheet.append_row(self.header)

        # Вставляем в каждый ряд сохраненную информацию
        for i in range(cols):
            for k in range(len(blank_headers_data)):
                try:
                    self.content[i].append(blank_headers_data[k][i])
                except IndexError:
                    self.content[i].append("")

        # вставляем в worksheet всю информацию
        worksheet.append_rows(self.content)

        print("Data was successfully transferred and saved! ({0})".format(self.worksheet_name))

    # данная функция занимается парсингом каждого оффера в списке
    def parse_offer(self, offer):
        self.offer_data = {}

        for key in self.parse_keywords:
            # сохраняем текущий ключ как sub_key
            sub_key = key
            if key in offer:
                value = offer[sub_key]
            else:
                # если ключ не объявлен в словаре keywords_parse, то оставляем данный слот пустым
                value = ""

            if type(value) != str:
                parse_info = self.parse_keywords[key] #записываем информацию которая нужна для парсинга 
                if type(parse_info) == str:
                    sub_key = self.parse_keywords[sub_key]
                    value = value[sub_key]
                elif type(parse_info) == dict:
                    self.parse_inner(value, parse_info["content"])
                    self.merge_values(parse_info)
                    value = None #чтобы потом вручную вставить значение в offer data
                elif type(parse_info) == list: #когда мы хотим 1 объект с несолькими значениями, например ID и соединить его с префиксом, потом взять след значение и сделать тоже самое 
                    for item in parse_info:
                        self.parse_inner(value, item["content"])
                        self.merge_values(item)
                    value = None
                elif type(parse_info) == set: #когда мы хотим из одного объекта взять несколько значений 
                    new_data = ""
                    for j in self.parse_keywords[sub_key]:
                        new_data = new_data + value[j]
                    value = new_data

            # Если наше значение не является пустым, то мы заносим его в массив обработанной информации
            if value is not None:
                self.offer_data[sub_key] = value

        # если ещё не создан header для нашей информации, то создаём его
        if not self.header_created:
            self.header_created = True
            self.header = list(self.offer_data.keys()) + self.blank_headers

        # вставляем пропарсенную инфу в общий массив
        # print(self.offer_data)
        self.content.append(list(self.offer_data.values())) #весь контент который мы пропарсили 

    #функция для глубинной проработки (когда нам нужны вложенные жанные) при использовании ключа content 
    def parse_inner(self, data, whitelist):
        if is_key_valid(whitelist, "content"): #используется рекурсия 
            self.parse_inner(data, whitelist["content"])
        else:
            for content_key in whitelist:
                if content_key.find(">") == -1:
                    self.offer_data[content_key] = data[content_key]
                else:
                    deep = content_key.split(">")
                    val = data 
                    last_key = deep[len(deep) - 1]
                    for key in deep:
                        val = val[key]

                    if not is_key_valid(self.offer_data, last_key):
                        content_key = last_key

                    self.offer_data[content_key] = val

    #эта функция которая работает при указании параметра merge, чтобы объединить значения которые мы получили при парсинге 
    def merge_values(self, parse_info):
        if not is_key_valid(parse_info, "merge"):
            return

        merged_data = ""
        items = parse_info["merge"]["items"]
        for i in range(len(items)):
            merged_data = merged_data + self.offer_data[items[i]]
            del self.offer_data[items[i]]

        self.offer_data[parse_info["merge"]["name"]] = merged_data

#смотрит есть ли ключ в словаре (для возможного дебагинга)
def is_key_valid(v_dict, key):
    if key in v_dict:
        return True

    return False