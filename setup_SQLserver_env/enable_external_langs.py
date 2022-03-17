from modules.DB_CRUD import engine

sql = """
sp_configure 'external scripts enabled', 1;

RECONFIGURE WITH override;
"""
engine.execute(sql)