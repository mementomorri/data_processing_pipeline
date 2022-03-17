import os
import configparser
from modules.global_variables import current_dirname

"""
check_log_size(int) -> None

Функция 'check_log_size' берет в качестве аргументов:
 'current_dirname' - строка с адресом проекта,
 'limit_log_size' - целое число, отвечающее за максимальный объем лог файла, еденица измерения байт
Функция не возвращает никаких данных, она вносит изменения в уже существующие, а именно меняет номер файла логгирования,
если его размер превышает допустимый. Такие изменения вносятся в файл конфигурации логгера, если файл меньше
максимального размера, то никак изменений не вносится. 
"""


def check_log_size(limit_log_size):
    logger_conf = configparser.RawConfigParser()  # Вызываем парсер файла конфигурации логгера
    logger_conf.read(current_dirname + "\config\logging.conf")  # Читаем файл конфигурации логгера
    path_to_log_file = logger_conf.get("handler_fileHandler", "args").split("\"")[1]  # Читаем путь к лог файлу
    if os.path.getsize(path_to_log_file) > int(limit_log_size):    # Сравниваем объем текущего лог файла с максимально допустимым
        ll=path_to_log_file.split(".")    # Разделяем строку на куски, чтобы достать номер лог файла
        log_file = logger_conf["handler_fileHandler"]   # Вызываем блок файла конфигурации отвечающий за адрес лог файла
        log_file["args"] = f"(r\"{ll[0]}.{ll[1]}.{int(ll[2])+1}\",)" # Прибаляем 1 к номеру лог файла
        with open(current_dirname+"\config\logging.conf", "w") as conf:    # Записываем изменения в файл конфигурации
            logger_conf.write(conf)