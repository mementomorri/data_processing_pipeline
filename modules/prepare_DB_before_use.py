from modules.DB_CRUD import engine
from modules.global_variables import configParser


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


def clean_tables_if_needed():
    sql = f"""
        IF EXISTS ( select * from dbo.{configParser.get("DBconnection", "te")} where tagpath='{configParser.get("IO_tags", "tagpath_gaps")}' )
        BEGIN
            DELETE * FROM dbo.{configParser.get("DBconnection", "te")} WHERE tagpath='{configParser.get("IO_tags", "tagpath_gaps")}';
        END
        GO 
    """
    engine.execute(sql)

    sql = f"""
        IF EXISTS ( select * from dbo.{configParser.get("DBconnection", "te")} where tagpath='{configParser.get("IO_tags", "tagpath_interpolated_data")}' )
        BEGIN
            DELETE * FROM dbo.{configParser.get("DBconnection", "te")} WHERE tagpath='{configParser.get("IO_tags", "tagpath_interpolated_data")}';
        END
        GO 
    """
    engine.execute(sql)

    sql = f"""
        IF EXISTS ( select * from dbo.{configParser.get("DBconnection", "table")} where tagid='{configParser.get("IO_tags", "tagid_gaps")}' )
        BEGIN
            DELETE * FROM dbo.{configParser.get("DBconnection", "table")} WHERE tagpath='{configParser.get("IO_tags", "tagid_gaps")}';
        END
        GO 
    """
    engine.execute(sql)

    sql = f"""
        IF EXISTS ( select * from dbo.{configParser.get("DBconnection", "table")} where tagid='{configParser.get("IO_tags", "tagid_interpolated_data")}' )
        BEGIN
            DELETE * FROM dbo.{configParser.get("DBconnection", "table")} WHERE tagpath='{configParser.get("IO_tags", "tagid_interpolated_data")}';
        END
        GO 
    """
    engine.execute(sql)
