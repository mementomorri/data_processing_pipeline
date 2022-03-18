import sys
sys.path.append("C:\\test")
from modules.prepare_DB_before_use import create_tables_if_not_exist, clean_tables_if_needed

create_tables_if_not_exist()
clean_tables_if_needed()