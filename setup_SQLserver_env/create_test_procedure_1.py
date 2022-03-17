from modules.DB_CRUD import engine

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