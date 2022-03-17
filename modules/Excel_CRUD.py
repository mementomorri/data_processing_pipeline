import pandas


def read_from_excel(excel_book, excel_sheet, columns=None):
    result = pandas.read_excel(excel_book, excel_sheet).sort_values(by="t_stamp")
    return result.iloc[:,:columns] if columns is not None else result