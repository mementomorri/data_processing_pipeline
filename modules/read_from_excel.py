import pandas


"""
read_from_excel(str, str, int) -> DataFrame

Функция 'read_from_excel' имеет в качестве аргументов
 'excel_book' - строка, содержащая имя файла эксель из которого мы читаем данные,
 'excel_sheet' - строка, содержащая имя страницы эксель из которой мы читаем данные,
 'columns' - целое число, количество столбцов которые мы читаем из источника.
 """

def read_from_excel(excel_book, excel_sheet, columns=None):
    result = pandas.read_excel(excel_book, excel_sheet).sort_values(by="t_stamp")
    # Читаем датафрейм из экселя используя внутренний метод Pandas
    return result.iloc[:,:columns] if columns is not None else result
