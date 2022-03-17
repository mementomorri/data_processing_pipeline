import configparser


current_dirname = "C:\\test"    # Относительный путь к проекту
configParser = configparser.RawConfigParser()   # Вызываем парсер конфигурационных файлов
configParser.read(current_dirname + "\config\config.ini")   # Читаем файл конфигураций хранящий глобальные переменные