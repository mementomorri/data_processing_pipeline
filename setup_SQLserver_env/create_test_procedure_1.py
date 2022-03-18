import urllib
from sqlalchemy import create_engine, MetaData
import configparser
import pyodbc


current_dirname = "C:\\test"    # Относительный путь к проекту
configParser = configparser.RawConfigParser()   # Вызываем парсер конфигурационных файлов
configParser.read(current_dirname + "\config\config.ini")   # Читаем файл конфигураций хранящий глобальные переменные

connection_url = configParser.get("DBconnection", "connection_url")  # Читаем первую часть URL для подключения к БД
conn = urllib.parse.quote_plus(
    "DRIVER={" + configParser.get("DBconnection", "driver") + "};SERVER=" + configParser.get("DBconnection","servername") +
    "; DATABASE=" + configParser.get("DBconnection", "database") + ";Trusted_Connection=yes")  # Собираем вторую часть
# URL строки для подключения к БД
engine = create_engine(connection_url + conn) # Собираем строку подключения вместе и создаем 'engine' для подключения к БД
meta = MetaData(bind=engine)  # Добавляем метаданные для срабатывания SQL запроса
MetaData.reflect(meta)
meta.create_all(engine)

sql = """

CREATE PROCEDURE test_procedure_1
AS
EXECUTE sp_execute_external_script @language = N'Python'
, @script = N'
import sys
sys.path.append("C:\\test")

from main import main_call
main_call()
'
"""
engine.execute(sql)
print("Процедура успешно создана!")