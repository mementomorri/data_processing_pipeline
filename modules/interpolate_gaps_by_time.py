import pandas as pd
import numpy as np

from main import configParser

"""
interpolate_gaps_by_time(DataFrame, int) -> DataFrame

Функция 'interpolate_gaps_by_time' имеет в качестве аргументов
 'organized_dataframe' - датафрейм с исходными данными,
 'median' - целое число, медианное значение по времени между промежутками времени.
В качестве результата функция возвращает датафрейм с линейно интерполированными данными, которые можно будет записать
в БД типа historian.
"""

def interpolate_gaps_by_time(organized_dataframe, median):
    organized_dataframe["t_stamp_"] = pd.to_datetime(organized_dataframe["t_stamp"],  # Добавляем колонку с читаемым
                                                     unit="ms")  # временем и датой
    organized_dataframe = organized_dataframe.set_index("t_stamp_").resample(f"{median // 1000}S").mean()  # Заполняем
    # данные по времени ресэмплингом
    organized_dataframe["t_stamp_"] = pd.to_datetime(organized_dataframe.index, unit="ms")  # Делаем столбец 't_stamp_'
    # индексным
    organized_dataframe["t_stamp__"] = organized_dataframe.index.values.astype(np.int64) // 10 ** 6  # Изменяем тип
    # данных для применения функции интерполяции
    organized_dataframe.reset_index(drop=True, inplace=True)  # Сбрасываем индексное поле
    organized_dataframe["tagid"] = configParser.get("IO_files", "tagid_for_interpolated_data")  # Указываем id тега в
    # который записываем
    organized_dataframe["dataintegrity"].fillna(value=192, inplace=True) # Заполняем пропуски в столбце 'dataintegrity'
    organized_dataframe["t_stamp"].fillna(value=organized_dataframe["t_stamp__"], inplace=True)  # Заполняем пропуски в
    # столбце 't_stamp' рассчитанными данными
    organized_dataframe.drop("t_stamp_", axis=1, inplace=True) # Выкидываем е нужные столбцы
    organized_dataframe.drop("t_stamp__", axis=1, inplace=True)
    organized_dataframe["floatvalue"] = organized_dataframe["floatvalue"].interpolate()  # Заполняем столбец 'floatvalue'
    # интерполяцией
    return organized_dataframe