import urllib
import pandas as pd

from sqlalchemy import create_engine, MetaData
from modules.global_variables import configParser

connection_url = configParser.get("DBconnection", "connection_url")  # Читаем первую часть URL для подключения к БД
conn = urllib.parse.quote_plus(
    "DRIVER={" + configParser.get("DBconnection", "driver") + "};SERVER=" + configParser.get("DBconnection","servername") +
    "; DATABASE=" + configParser.get("DBconnection", "database") + ";Trusted_Connection=yes")  # Собираем вторую часть
# URL строки для подключения к БД
engine = create_engine(connection_url + conn) # Собираем строку подключения вместе и создаем 'engine' для подключения к БД
meta = MetaData(bind=engine)  # Добавляем метаданные для срабатывания SQL запроса
MetaData.reflect(meta)
meta.create_all(engine)

"""
delete_table_from_DB(int, bool) -> None

Функция 'delete_table_from_DB' берет в качестве аргументов
 'n_of_rows_to_delete' - целое число, количество строк, которое нужно удалить
 'delete_from_top' - булиево значение True/False, отвечает за направление удаления, если True то удаляем верхние строки
если False, то удаляем нижние строки.
Функция не возвращает никакие данные, она исполняет запрос на удаление строк данных из БД, проверить это можно только 
чтением данных из таблицы.
"""

def delete_table_from_DB(n_of_rows_to_delete=None, delete_from_top=False):
    sql = f'DELETE {"top" if delete_from_top else "bottom"} {n_of_rows_to_delete} * FROM {configParser.get("DBconnection", "table")}' if n_of_rows_to_delete is not None else f'DELETE * FROM {configParser.get("DBconnection", "table")}'
    engine.execute(sql)  # Собирает строку SQL запроса и выполняет получаеный запрос

"""
read_from_DB(int, int, bool) -> DataFrame

Функция 'read_from_DB' берет в качестве аргументов
 'n_of_rows_to_read' - целое число, количество строк, которое нужно прочитать
 'n_of_columns_to_read' - целое число, количество столбцов, которое нужно прочитать
 'read_from_top' - булиево значение True/False, отвечает за направление чтения, если True то читаем верхние строки
если False, то удаляем нижние строки. 
Функция возвращает датафрэйм, она исполняет запрос на чтение строе данных из БД. По умолчанию читаем всю таблицу.
"""

def read_from_DB(n_of_rows_to_read=None, n_of_columns_to_read=None, read_from_top=False):
    sql = f"""
    SELECT {"top" if read_from_top else "bottom"} {n_of_rows_to_read} *
    FROM {configParser.get("DBconnection", "table")}
    """  # Собирает строку SQL запроса и выполняет получаеный запрос
    if n_of_columns_to_read is not None and n_of_rows_to_read is not None:
        return pd.read_sql(sql, engine).iloc[:, :n_of_columns_to_read]  # Если первые два аргкумента функции не None,
        # то читаем только нужные части таблицы, в противном случае читаем всю таблицу
    else:
        return pd.read_sql(f"SELECT * FROM {configParser.get('DBconnection', 'table')}", engine)

"""
write_to_DB(DataFrame) -> None

Функция 'write_to_DB' берет в качестве аргументов
 'input_data' - датафрейм, таблица которую нужно записать в БД.
Функция не возвращает никакие данные, она исполняет запрос на запись таблицы в БД, проверить это можно только 
чтением данных из таблицы.
"""

def write_to_DB(input_data):
    input_data.to_sql(configParser.get("DBconnection", "table"), con=engine, if_exists="append", index=False)
    # Записываем датафрейм в БД используя внутренний метод Pandas