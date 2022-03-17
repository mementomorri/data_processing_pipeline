import configparser
import logging.config

from modules.get_unusual_data_segments import organize_data
from modules.read_from_excel import read_from_excel
from modules.DB_CRUD import write_to_DB
from modules.check_log_size import check_log_size
from modules.interpolate_gaps_by_time import interpolate_gaps_by_time

current_dirname = "C:\\test"    # Относительный путь к проекту
configParser = configparser.RawConfigParser()   # Вызываем парсер конфигурационных файлов
configParser.read(current_dirname + "\config\config.ini")   # Читаем файл конфигураций хранящий глобальные переменные
limit_log_size = configParser.get("Logging", "limit_log_size")    # Инициализируем максимальный размер лог файла 10 МБ
debugging_mode = configParser.get("Logging", "debugging_mode")    # Инициальзируем переключение режима дебаггинга
columns_with_data = list(configParser.get("IO_files", "columns_with_data"))    # Вызываем список колонок с
                                                                               # расчетными данными
n_columns_to_read = configParser.get("IO_files", "n_columns_to_read")    # Указываем сколько колонок читать

logging.config.fileConfig(current_dirname +
                          configParser.get("Logging", "config_path"))   # Загружаем файл конфигурации логгера
logger = logging.getLogger("case_info")    # Вызываем объект логгера "case_info"
check_log_size(limit_log_size)    # Проверяем объем лог файла с помощью функции 'check_log_filse'

"""
Функция 'main_call' не имеет аргкментов и не возвращает никаких данных, её основной смысл инициализация программы
и её выполнение, входные данные к программе задекларированны в файле конфигурации 'config.ini', а их чтение 
из файла конфигурации произведеного выше соответственно далее представлен только исполняемый код.
Для выполнения программы нужно импортировать этот модуль и вызвать эту функцию.
"""

if __name__ == '__main__':
    logger.info("Test case №1 started ")    # Информируем о времени и дате начала программы
    unprocessed_dataframe = read_from_excel(current_dirname + configParser.get("IO_files", "input_excel"),
                                            configParser.get("IO_files", "input_sheet"), n_columns_to_read)
    # Берем исходные данные из "book2.xlsx - Sheet3" и читаем 7 первых столбцов
    try:
        if debugging_mode:  # Если мы находимся в режиме дебага, то добавляем больше служебной информации в лог файл
            logger.debug("--- 1 Starting to write unprocessed data to database")
        write_to_DB(configParser, unprocessed_dataframe)   # Записываем исходные данные в БД вызвав функцию 'write_to_DB'
    except Exception as error_code:     # Если встречаем ошибку, то сообщаем об этом в лог файл не зависимо от того
                                        # находимся ли мы в режиме дебага
        logger.error("Unable to write to database."+error_code)
    else:
        if debugging_mode:  # Если мы находимся в режиме дебага, то добавляем больше служебной информации в лог файл
            logger.debug("--- 1 Unprocessed data written successfully")

    try:
        if debugging_mode:   # Если мы находимся в режиме дебага, то добавляем больше служебной информации в лог файл
            logger.debug("--- 2 Starting to reorganize data and writing it to database")
        gaps_dataframe, median, percent = organize_data(unprocessed_dataframe, True)  # Считаем количество пропусков
        # в исходныйх данных, критерием отбора в функции 'organize_data' являются временные промежутки
        gaps_dataframe["tagid"] = configParser.get("IO_files", "tagid_for_gaps")  # id тэга, который будет отвечать за
        # количество пропусков
        for n in columns_with_data:   # Выкидываем рачетные столбцы с помощью цикла 'for', чтобы записать
            gaps_dataframe.drop(n, axis=1, inplace=True) # обработанные данные в БД
        gaps_dataframe["floatvalue"] = gaps_dataframe["N_skipped"] # Записываем пропуски в колонку 'floatvalue'
        gaps_dataframe.drop("N_skipped", axis=1, inplace=True)  # а затем выкидываем поледнюю расчетную колонку
        write_to_DB(configParser, gaps_dataframe)  # Записываем пропуски в БД
    except Exception as error_code:     # Если встречаем ошибку, то сообщаем об этом в лог файл не зависимо от того
                                        # находимся ли мы в режиме дебага
        logger.error("Error occurred while processing dataframe." + error_code)
    else:
        if debugging_mode:  # Если мы находимся в режиме дебага, то добавляем больше служебной информации в лог файл
            logger.debug("--- 2 Processed data written successfully")

    try:
        if debugging_mode: # Если мы находимся в режиме дебага, то добавляем больше служебной информации в лог файл
            logger.debug("--- 3 Interpolating dataframe and writing results to database")
        interpolated_dataframe = interpolate_gaps_by_time(unprocessed_dataframe.copy(), median)  # Заполняем пропуски
        write_to_DB(interpolated_dataframe)  # в данных методом линейной интерполяции
    except Exception as error_code:     # Если встречаем ошибку, то сообщаем об этом в лог файл не зависимо от того
                                        # находимся ли мы в режиме дебага
        logger.error("Error occurred while interpolating dataframe." + error_code)
    else:
        if debugging_mode:  # Если мы находимся в режиме дебага, то добавляем больше служебной информации в лог файл
            logger.debug("--- 3 Interpolation and loading to database finished successfully")
        logger.info("Test case №1 finished") # Сообщаем в лог дату и время завершения программы