import pandas as pd

"""
organize_data(DataFrame, bool) -> DataFrame

Функция 'organize_data' имеет в качестве аргументов
 'input_data' - датафрейм с исходными данными,
 'show_info' - булиево значение отвечающее за количество выводимой информации, если True, то помимо таблицы с пропусками
возвращает медиану и процент пропусков. 
"""


def organize_data(input_data, show_info=False):  # Функция обработки таблицы записанной в указанный файл экселя
    if len(input_data.index) <= 2 or input_data.empty:  # Проверяем длинну датафрэйма, если в данных лишь одна строка или они пустые, то сразу возвращаем исключение
        return Exception("Not enough input data provided")

    result = input_data.copy()
    result['human_readable_stamp'] = result['t_stamp'].apply(lambda x: pd.to_datetime(x, unit='ms')) # Делаем колонку human_readable_stamp с читаемым форматом временного штампа
    result['t_gap'] = result['t_stamp'] - result['t_stamp'].shift(1)  # Вычисляем временной интервал между поступившими данными t_gap
    gap_median = result['t_gap'].median()  # Вычисляем медианное значение
    result['N_skipped'] = result['t_gap'].apply(  # Считаем количество попущенных временных интервалов и записываем в столбец N_skipped
        lambda x: 0 if (pd.isna(x) or int(x / gap_median) <= 1) else int(x / gap_median)
    )
    sum_of_gaps = result['N_skipped'].sum()  # Считаем процент аномальных значений относительно общего числа запсей
    percent_of_gaps = sum_of_gaps / len(result.index) * 100
    result["median_gap"] = round(gap_median, 3)
    result["percent_of_gaps"] = percent_of_gaps

    if show_info:
        return result, round(gap_median, 3), percent_of_gaps  # Если параметр show_info имеет значение True, то возвращаем
    else:  # округленное медианное значение и процент аномальных значений
        return result
