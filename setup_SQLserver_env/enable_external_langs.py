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
sp_configure 'external scripts enabled', 1;

RECONFIGURE WITH override;
"""
engine.execute(sql)
print("Теперь SQLserver может запускать скрипты Python")