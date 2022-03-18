import pandas as pd
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



def create_tables_if_not_exist():
    sql = f"""
    IF OBJECT_ID('dbo.{configParser.get("DBconnection", "te")}', 'U') IS NULL CREATE TABLE [dbo].[{configParser.get("DBconnection", "te")}](
        [id] [int] IDENTITY(1,1) NOT NULL,
        [tagpath] [nvarchar](255) NULL,
        [scid] [int] NULL,
        [datatype] [int] NULL,
        [querymode] [int] NULL,
        [created] [bigint] NULL,
        [retired] [bigint] NULL,
    PRIMARY KEY CLUSTERED 
    (
        [id] ASC
    )WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
    ) ON [PRIMARY]; 
    """
    engine.execute(sql)

    sql = f"""
    IF OBJECT_ID('dbo.{configParser.get("DBconnection", "table")}', 'U') IS NULL CREATE TABLE [dbo].[{configParser.get("DBconnection", "table")}](
        [tagid] [bigint] NULL,
        [intvalue] [float] NULL,
        [floatvalue] [bigint] NULL,
        [stringvalue] [float] NULL,
        [datevalue] [float] NULL,
        [dataintegrity] [bigint] NULL,
        [t_stamp] [bigint] NULL
    ) ON [PRIMARY]; 
        """
    engine.execute(sql)
    print('Проверка наличия нужных таблиц прошла успешно')


def clean_tables_if_needed():
    sql = f"""
        INSERT INTO [dbo].[{configParser.get("DBconnection", "te")}]
           ([tagpath]
           ,[scid]
           ,[datatype]
           ,[querymode]
           ,[created]
           ,[retired])
     VALUES
           ('{configParser.get("IO_tags", "tagpath_gaps")}'
           ,1
           ,1
           ,3
           ,{configParser.get("IO_tags", "tag_creation_time")}
           , NULL)
        """
    if not(pd.read_sql(f'SELECT * FROM dbo.{configParser.get("DBconnection", "te")} WHERE tagpath=\'{configParser.get("IO_tags", "tagpath_gaps")}\'', engine).empty):
        engine.execute(f"""
        DELETE FROM dbo.{configParser.get("DBconnection", "te")} WHERE tagpath='{configParser.get("IO_tags", "tagpath_gaps")}';
        """)
    engine.execute(sql)

    sql = f"""
        INSERT INTO [dbo].[{configParser.get("DBconnection", "te")}]
           ([tagpath]
           ,[scid]
           ,[datatype]
           ,[querymode]
           ,[created]
           ,[retired])
     VALUES
           ('{configParser.get("IO_tags", "tagpath_interpolated_data")}'
           ,1
           ,1
           ,3
           ,{configParser.get("IO_tags", "tag_creation_time")}
           , NULL)
        """
    if not(pd.read_sql(f'SELECT * FROM dbo.{configParser.get("DBconnection", "te")} WHERE tagpath=\'{configParser.get("IO_tags", "tagpath_interpolated_data")}\'', engine).empty):
        engine.execute(f"""
        DELETE FROM dbo.{configParser.get("DBconnection", "te")} WHERE tagpath='{configParser.get("IO_tags", "tagpath_interpolated_data")}';
        """)
    engine.execute(sql)

    if not(pd.read_sql(f'SELECT * FROM dbo.{configParser.get("DBconnection", "table")} WHERE tagid=\'{configParser.get("IO_tags", "tagid_gaps")}\'', engine).empty):
        engine.execute(f"""
        DELETE FROM dbo.{configParser.get("DBconnection", "table")} WHERE tagid='{configParser.get("IO_tags", "tagid_gaps")}';
        """)

    if not(pd.read_sql(f'SELECT * FROM dbo.{configParser.get("DBconnection", "table")} WHERE tagid=\'{configParser.get("IO_tags", "tagid_interpolated_data")}\'', engine).empty):
        engine.execute(f"""
        DELETE FROM dbo.{configParser.get("DBconnection", "table")} WHERE tagid='{configParser.get("IO_tags", "tagid_interpolated_data")}';
        """)
    print(f'Проверка наличия нужных записей в таблице {configParser.get("DBconnection", "te")} прошла успешно, '
          f'а также удаление ненужных записей в таблице {configParser.get("DBconnection", "table")} проведено успешно')

if __name__ == '__main__':
    create_tables_if_not_exist()
    clean_tables_if_needed()