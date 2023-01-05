import pandas as pd
import csv

# import psycopg2
import psycopg2 as pg
from sqlalchemy import create_engine
import urllib.parse
import codecs
import configparser
import os
import subprocess
import logging
import datetime
import multiprocessing
# import pyodbc
import pymssql


class LoadProperties:
    def __init__(self):
        try:
            configParser = configparser.RawConfigParser()
            configFilePath = 'dbconfig.properties'
            configParser.read(configFilePath)

            self.sqlserver_host = configParser.get('PARAMETERS', 'sqlserver_host')
            self.sqlserver_port = configParser.get('PARAMETERS', 'sqlserver_port')
            self.sqlserver_database = configParser.get('PARAMETERS', 'sqlserver_database')
            self.sqlserver_schema = configParser.get('PARAMETERS', 'sqlserver_schema')
            self.sqlserver_username = configParser.get('PARAMETERS', 'sqlserver_username')
            self.sqlserver_password = configParser.get('PARAMETERS', 'sqlserver_password')

            self.postgres_host = configParser.get('PARAMETERS', 'postgres_host')
            self.postgres_port = configParser.get('PARAMETERS', 'postgres_port')
            self.postgres_database = configParser.get('PARAMETERS', 'postgres_database')
            self.postgres_schema = configParser.get('PARAMETERS', 'postgres_schema')
            self.postgres_username = configParser.get('PARAMETERS', 'postgres_username')
            self.postgres_password = configParser.get('PARAMETERS', 'postgres_password')

            self.download_type = configParser.get('PARAMETERS', 'download_type')
            self.out_dir = configParser.get('PARAMETERS', 'out_dir')
            createfolder(self.out_dir)
            createfolder(self.out_dir + "postgres\\")
            createfolder(self.out_dir + "sqlserver\\")
            createfolder(self.out_dir + "result\\")

            self.max_processes = configParser.get('PARAMETERS', 'MAX_THREADS')
            self.table_list = configParser.get('PARAMETERS', 'table_list').replace(' ', '').strip().lower()


        except Exception as e:
            print(e)


def createfolder(o_dir):
    try:
        if not os.path.exists(o_dir):
            os.makedirs(o_dir)
    except OSError:
        print("Creation of the directory %s failed" % o_dir)


def log_config(logfile):
    FileFormatter = logging.Formatter("%(asctime)s [%(levelname)-8.8s]  %(message)s")
    # FileFormatter = logging.Formatter("%(asctime)s [%(funcName)-12.12s] [%(levelname)-8.8s]  %(message)s")
    ConsoleFormatter = logging.Formatter("[%(asctime)s] : %(message)s")

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    consoleHandler = logging.StreamHandler()
    consoleHandler.setFormatter(ConsoleFormatter)
    logger.addHandler(consoleHandler)

    fileHandler = logging.FileHandler(logfile, mode='a', encoding=None, delay=True)
    fileHandler.setFormatter(FileFormatter)
    logger.addHandler(fileHandler)

    return logger


def download_postgres_data(properties, tablename, columnnames, orderbyclause):
    logger = logging.getLogger(logFileName)
    try:
        retry = 1
        retry_flag = True
        vretry = 1
        df = pd.DataFrame()
        # print ("host=" +properties.postgres_host +" dbname=" +properties.postgres_database +" user=" +properties.postgres_username +" password=" +properties.postgres_password)
        while retry_flag:
            try:
                # engine = pg.connect("host=" +properties.postgres_host +" dbname=" +properties.postgres_database +" user=" +properties.postgres_username +" password=" +properties.postgres_password)
                engine = pg.connect(user=properties.postgres_username, password=properties.postgres_password,
                                    host=properties.postgres_host, port=properties.postgres_port,
                                    database=properties.postgres_database)

                fileName = properties.out_dir + "postgres\\" + tablename + ".dat"
                logger.info("tablename = " + tablename + " fileName ====" + fileName)

                query = "select " + columnnames + " from " + properties.postgres_schema + "." + tablename + orderbyclause  # +" limit 100000"
                # query = "select * from dbo.app_shortcut_details"
                logger.info("query = " + query + " fileName ====" + fileName)
                pgdf = pd.read_sql_query(query, engine)

                with codecs.open(fileName, "w", encoding="utf-8") as f:
                    pgdf.to_csv(f, index=False, encoding="utf-8", line_terminator="\r\n", header=columnnames.lower(),
                                sep="|", chunksize=10000)
                    f.close()

                del (pgdf)

                retry_flag = False
            except:
                if retry < vretry:
                    retry = retry + 1
                else:
                    retry_flag = False
                    raise

    except Exception as ex:
        logger.info("Issue with connecting to Postgres DB")
        logger.info(ex)
        raise


def download_postgres_query_data(properties, tablename, columnnames, queryinput):
    logger = logging.getLogger(logFileName)
    try:
        retry = 1
        retry_flag = True
        vretry = 1
        df = pd.DataFrame()
        # print("host=" + properties.postgres_host + " dbname=" + properties.postgres_database + " user=" + properties.postgres_username + " password=" + properties.postgres_password)
        while retry_flag:
            try:
                # engine = pg.connect("host=" +properties.postgres_host +" dbname=" +properties.postgres_database +" user=" +properties.postgres_username +" password=" +properties.postgres_password)
                engine = pg.connect(user=properties.postgres_username, password=properties.postgres_password,
                                    host=properties.postgres_host, port=properties.postgres_port,
                                    database=properties.postgres_database)

                fileName = properties.out_dir + "postgres\\" + tablename + ".dat"
                logger.info("tablename = " + tablename + " fileName ====" + fileName)

                query = queryinput
                logger.info("query = " + query + " fileName ====" + fileName)
                pgdf = pd.read_sql_query(query, engine)

                with codecs.open(fileName, "w", encoding="utf-8") as f:
                    pgdf.to_csv(f, index=False, encoding="utf-8", line_terminator="\r\n", header=columnnames.lower(),
                                sep="|", chunksize=10000)
                    f.close()

                del (pgdf)

                retry_flag = False
            except:
                if retry < vretry:
                    retry = retry + 1
                else:
                    retry_flag = False
                    raise

    except Exception as ex:
        logger.info("Issue with connecting to Postgres DB")
        logger.info(ex)
        raise


def download_sqlserver_data(properties, tablename, columnnames, orderbyclause):
    logger = logging.getLogger(logFileName)
    try:
        retry = 1
        retry_flag = True
        vretry = 1

        df = pd.DataFrame()
        while retry_flag:
            try:

                driver = '{ODBC Driver 17 for SQL Server}'
                login_timeout = '60'

                fileName = properties.out_dir + "sqlserver\\" + tablename + ".dat"
                logger.info("tablename = " + tablename + " fileName ====" + fileName)

                connection_string = urllib.parse.quote_plus(
                    'DRIVER=' + driver + ';SERVER=' + properties.sqlserver_host + ';DATABASE=' + properties.sqlserver_database + ';UID=' + properties.sqlserver_username + ';PWD=' + properties.sqlserver_password)
                # connection_string = f"mssql+pymssql://{properties.sqlserver_username}:{properties.sqlserver_password}@{properties.sqlserver_host}/?charset=utf8"
                connection_string = f"mssql+pymssql://{properties.sqlserver_username}:{properties.sqlserver_password}@{properties.sqlserver_host}/{properties.sqlserver_username}"
                # engine = create_engine(connection_string, connect_args={'connect_timeout': login_timeout})
                engine = create_engine(connection_string)

                query = "select " + columnnames + " from " + properties.sqlserver_schema + "." + tablename + "  " + orderbyclause
                logger.info("query = " + query)
                pgdf = pd.read_sql_query(query, engine)

                with codecs.open(fileName, "w", encoding="utf-8") as f:
                    pgdf.to_csv(f, index=False, encoding="utf-8", line_terminator="\r\n", header=columnnames.lower(),
                                sep="|", chunksize=10000)
                    f.close()

                del (pgdf)
                engine.dispose()

                retry_flag = False
            except:
                if retry < vretry:
                    retry = retry + 1
                else:
                    retry_flag = False
                    raise

    except Exception as ex:
        logger.info("Issue connecting with SQLSERVER DB")
        logger.info(ex)
        raise


def download_sqlserver_query_data(properties, tablename, columnnames, queryinput):
    logger = logging.getLogger(logFileName)
    try:
        retry = 1
        retry_flag = True
        vretry = 1

        df = pd.DataFrame()
        while retry_flag:
            try:

                driver = '{ODBC Driver 17 for SQL Server}'
                login_timeout = '60'

                fileName = properties.out_dir + "sqlserver\\" + tablename + ".dat"
                logger.info("tablename = " + tablename + " fileName ====" + fileName)

                connection_string = urllib.parse.quote_plus(
                    'DRIVER=' + driver + ';SERVER=' + properties.sqlserver_host + ';DATABASE=' + properties.sqlserver_database + ';UID=' + properties.sqlserver_username + ';PWD=' + properties.sqlserver_password)
                # connection_string = f"mssql+pymssql://{properties.sqlserver_username}:{properties.sqlserver_password}@{properties.sqlserver_host}/?charset=utf8"
                connection_string = f"mssql+pymssql://{properties.sqlserver_username}:{properties.sqlserver_password}@{properties.sqlserver_host}/{properties.sqlserver_username}"
                # engine = create_engine(connection_string, connect_args={'connect_timeout': login_timeout})
                engine = create_engine(connection_string)

                query = queryinput
                logger.info("query = " + query)
                pgdf = pd.read_sql_query(query, engine)

                with codecs.open(fileName, "w", encoding="utf-8") as f:
                    pgdf.to_csv(f, index=False, encoding="utf-8", line_terminator="\r\n", header=columnnames.lower(),
                                sep="|", chunksize=10000)
                    f.close()

                del (pgdf)
                engine.dispose()

                retry_flag = False
            except:
                if retry < vretry:
                    retry = retry + 1
                else:
                    retry_flag = False
                    raise

    except Exception as ex:
        # logger.info("Issue connecting with SQLSERVER DB")
        print(ex)
        # logger.info(ex)
        raise


def getPGConnection(source_user, source_pwd, source_hostname, source_port, source_dbname):
    conn = ''
    try:
        conn = psycopg2.connect(user=source_user, password=source_pwd, host=source_hostname, port=source_port,
                                database=source_dbname)
    except Exception as e:
        print("error found in Postgres DB connection", e)
    return conn


def getSQLconnection(target_user, target_pwd, target_hostname, target_port, target_database_name):
    conn = ''
    try:
        conn = pymssql.connect(target_hostname, target_user, target_pwd, target_database_name)
        # cursor = conn.cursor(as_dict=True)

        # sql = 'DRIVER={ODBC Driver 13 for SQL Server};SERVER=' + target_hostname + ';DATABASE=' + target_database_name + ';UID=' + target_user + ';PWD=' + target_pwd
        # print ("sql -> ", sql)
        # conn = pyodbc.connect(sql, autocommit=True)
    except Exception as e:
        print("error found in SQL DB connection", e)
        # print("")
    return conn


def getColumnNames(properties, tableName, targetdb):
    logger = logging.getLogger(logFileName)

    columnsname = 'column_name'
    columns = ''

    try:
        sqlconn = getSQLconnection(properties.sqlserver_username, properties.sqlserver_password,
                                   properties.sqlserver_host,
                                   properties.sqlserver_port, properties.sqlserver_database)

        if (targetdb == 'postgres'):
            columnsname = "case when (data_type = 'USER-DEFINED') then " \
                          "case when udt_name = 'datetime' then 'to_char('||column_name||', ''YYYY-MM-DD HH24:MI:SS.ms'') as ' || column_name " \
                          "when udt_name = 'bit' then column_name || '::integer::boolean as ' || column_name " \
                          "else column_name end else column_name end as column_name"
            # "when udt_name = 'bit' then ' || column_name || '::integer::boolean else ' || column_name || ' as ' || column_name " \
            # "case when data_type = 'datetime' then " +'to_char('+column_name+', ''YYYY-MM-DD HH24:MI:SS.MS'')' +" else " +columnsname +" end as column_name "
            # 'column_name' #"case when data_type = 'boolean' then column_name || '::integer as ' || column_name else column_name end as column_name"

            sqlconn = pg.connect(user=properties.postgres_username, password=properties.postgres_password,
                                 host=properties.postgres_host, port=properties.postgres_port,
                                 database=properties.postgres_database)

        if (targetdb == 'sqlserver'):
            columnsname = " case when DATA_TYPE = 'varbinary' then " \
                          "'concat(''0x'', lower(CONVERT(VARCHAR(1000), ' +COLUMN_NAME +', 2))) as '+ COLUMN_NAME " \
                          " when DATA_TYPE = 'datetime' then +' convert(varchar(25), ' +COLUMN_NAME +', 121)' " \
                          " else COLUMN_NAME end as COLUMN_NAME "

        cursor = sqlconn.cursor()

        query = "select " + columnsname \
                + " from information_schema.columns where table_name = '" + tableName + "' and table_schema = '" + (
                    properties.sqlserver_schema) + "' order by ordinal_position"
        # print ("\ngetColumnNames query => " +query)

        cursor.execute(query)
        records = cursor.fetchall()
        i = 1

        for row in records:
            if (i < len(records)):
                columns = columns + row[0].lower() + ","
            else:
                columns = columns + row[0].lower()
            i = i + 1

        cursor.close()
        sqlconn.close()

    except Exception as e:
        # logger.info("issue getColumnNames")
        # logger.info(e)
        print("")
    return columns


def getOrderByClause(properties, tableName):
    logger = logging.getLogger(logFileName)

    columns = ''
    try:
        sqlconn = getSQLconnection(properties.sqlserver_username, properties.sqlserver_password,
                                   properties.sqlserver_host,
                                   properties.sqlserver_port, properties.sqlserver_database)
        # sqlconn = pg.connect(user=properties.postgres_username, password=properties.postgres_password, host=properties.postgres_host, port=properties.postgres_port, database=properties.postgres_database)
        cursor = sqlconn.cursor()

        query = "SELECT distinct top 1 lower(column_name) as columnname, TC.CONSTRAINT_TYPE, KU.ORDINAL_POSITION FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS TC INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KU  ON TC.CONSTRAINT_TYPE in ('PRIMARY KEY', 'UNIQUE')  AND TC.CONSTRAINT_NAME = KU.CONSTRAINT_NAME AND KU.table_name='" + tableName + "' ORDER BY  TC.CONSTRAINT_TYPE, KU.ORDINAL_POSITION"
        # ("\n" +query)

        cursor.execute(query)
        records = cursor.fetchall()
        i = 1

        for row in records:
            if (i < len(records)):
                columns = columns + row[0].lower() + ","
            else:
                columns = columns + row[0].lower()
            i = i + 1

        cursor.close()
        sqlconn.close()

    except Exception as e:
        # logger.info("issue getOrderByClause")
        # logger.info(e)
        print("")
    return columns


logFileName = 'log-download-tables.log'
log_config(logFileName)

if __name__ == '__main__':

    logger = logging.getLogger(logFileName)
    logger.info("\n\n\n")

    try:
        logger.info("\n******* Started   - Download Data from DB to DAT files ***********\n")

        properties = LoadProperties()

        tableslist = properties.table_list.split(",")
        download_type = properties.download_type

        # Define IPC manager
        manager = multiprocessing.Manager()

        num_processes = int(properties.max_processes)

        # Declare the processes pool
        pool = multiprocessing.Pool(processes=num_processes)

        # Declare the processes pool
        pgpool = multiprocessing.Pool(processes=num_processes)

        if (tableslist != "" and download_type == "tables"):

            logger.info("Total Tables for Download " + str(len(tableslist)))

            logger.info("\n\n     ******* Started   - Data download for SQL Server ***********")
            for table in tableslist:
                tableName = table.strip().lower()
                columns = getColumnNames(properties, tableName, 'sqlserver')

                orderbyclause = getOrderByClause(properties, tableName)
                # orderbyclause = " 1 "
                if (orderbyclause == ''):
                    orderbyclause = columns
                    orderbyclause = ' 1,2 '

                if (table == 'filermst_prop'):
                    columns = columns.replace('hashvalue,', '')
                    columns = columns.replace('accession,', ' LTRIM(RTRIM(accession)) as accession,')
                    columns = columns.replace('rpt_date,', ' LTRIM(RTRIM(rpt_date)) as rpt_date,')
                if (table == 'people_jobs'):
                    columns = columns.replace('department,', ' LTRIM(RTRIM(department)) as department,')
                if (table == 'people'):
                    columns = columns.replace('salutation,', ' LTRIM(RTRIM(salutation)) as salutation,')
                if (table == 'secmas'):
                    columns = columns.replace('acronym,', ' LTRIM(RTRIM(acronym)) as acronym,')
                if (table == 'filermst'):
                    columns = columns.replace('zip_code,', ' LTRIM(RTRIM(zip_code)) as zip_code,')

                orderbyclause = " order by  " + orderbyclause
                logger.info('tableName = ' + tableName)
                pool.apply_async(download_sqlserver_data, args=(properties, tableName, columns, orderbyclause))

            pool.close()
            pool.join()
            logger.info("\n\n     ******* Completed - Data download for SQL Server *********** ")

            logger.info("\n\n     ******* Started   - Data download for Postgres *********** ")
            for table in tableslist:
                tableName = table.strip().lower()
                columns = getColumnNames(properties, tableName, 'postgres')

                orderbyclause = getOrderByClause(properties, tableName)
                if (orderbyclause == ''):
                    orderbyclause = columns
                    orderbyclause = ' 1,2 '

                if (table == 'filermst_prop'):
                    columns = columns.replace('hashvalue,', '')
                    columns = columns.replace('accession,', ' TRIM(accession) as accession,')
                    columns = columns.replace('rpt_date,', ' TRIM(rpt_date) as rpt_date,')
                if (table == 'people_jobs'):
                    columns = columns.replace('department,', ' TRIM(department) as department,')
                if (table == 'people'):
                    columns = columns.replace('salutation,', ' TRIM(salutation) as salutation,')
                if (table == 'secmas'):
                    columns = columns.replace('acronym,', ' TRIM(acronym) as acronym,')
                if (table == 'filermst'):
                    columns = columns.replace('zip_code,', ' TRIM(zip_code) as zip_code,')

                orderbyclause = " order by  " + orderbyclause
                columns = columns.replace('offset', '"offset"')
                orderbyclause = orderbyclause.replace('offset', '"offset"')
                logger.info('tableName = ' + tableName)
                pgpool.apply_async(download_postgres_data, args=(properties, tableName, columns, orderbyclause))

            pgpool.close()
            pgpool.join()
            logger.info("\n\n     ******* Completed - Data download for Postgres ***********\n")

        if (download_type == "sqlfiles"):

            logger.info("\n\n     ******* Started   - Data download using SQL Files for SQL Server ***********")

            # Using readlines()
            sqlfile = open('sqlserver.sql', 'r')
            sqlLines = sqlfile.readlines()
            sqllist = []

            count = 0
            # Strips the newline character
            for line in sqlLines:
                count += 1
                sqllist.append(line.strip())

            # Using readlines()
            pgfile = open('postgres.sql', 'r')
            pgLines = pgfile.readlines()
            pglist = []

            count = 0
            # Strips the newline character
            for line in pgLines:
                count += 1
                pglist.append(line.strip())

            count = 0

            logger.info("\n\n     ******* Started - Data download using SQL Query Files for SQL Server *********** ")
            for qry in sqllist:

                sqlqueryinput = qry
                tableName = sqlqueryinput.split("from ")[1].split(" ")[0].replace("(nolock)", "").replace("nolock", "")
                tableNameLength = len(tableName.split("."))
                tableName = tableName.split(".")[tableNameLength - 1]
                columns = getColumnNames(properties, tableName, 'sqlserver')
                count = count + 1

                if (tableName == 'filermst_prop'):
                    columns = columns.replace('hashvalue,', '')
                    columns = columns.replace('accession,', ' LTRIM(RTRIM(accession)) as accession,')
                    columns = columns.replace('rpt_date,', ' LTRIM(RTRIM(rpt_date)) as rpt_date,')
                if (tableName == 'people_jobs'):
                    columns = columns.replace('department,', ' LTRIM(RTRIM(department)) as department,')
                if (tableName == 'people'):
                    columns = columns.replace('salutation,', ' LTRIM(RTRIM(salutation)) as salutation,')
                if (tableName == 'secmas'):
                    columns = columns.replace('acronym,', ' LTRIM(RTRIM(acronym)) as acronym,')
                if (tableName == 'filermst'):
                    columns = columns.replace('zip_code,', ' LTRIM(RTRIM(zip_code)) as zip_code,')

                sqlqueryinput = sqlqueryinput.replace("*", columns)

                pool.apply_async(download_sqlserver_query_data, args=(properties, tableName, columns, sqlqueryinput))

            pool.close()
            pool.join()
            logger.info("\n\n     ******* Completed - Data download using SQL Query Files for SQL Server *********** ")

            logger.info("\n\n     ******* Started - Data download using SQL Query Files for Postgres *********** ")
            for qry in pglist:

                sqlqueryinput = qry
                tableName = sqlqueryinput.split("from ")[1].split(" ")[0].replace("(nolock)", "").replace("nolock", "")
                tableNameLength = len(tableName.split("."))
                tableName = tableName.split(".")[tableNameLength - 1]
                columns = getColumnNames(properties, tableName, 'postgres')
                count = count + 1

                if (tableName == 'filermst_prop'):
                    columns = columns.replace('hashvalue,', '')
                    columns = columns.replace('accession,', ' TRIM(accession) as accession,')
                    columns = columns.replace('rpt_date,', ' TRIM(rpt_date) as rpt_date,')
                if (tableName == 'people_jobs'):
                    columns = columns.replace('department,', ' TRIM(department) as department,')
                if (tableName == 'people'):
                    columns = columns.replace('salutation,', ' TRIM(salutation) as salutation,')
                if (tableName == 'secmas'):
                    columns = columns.replace('acronym,', ' TRIM(acronym) as acronym,')
                if (tableName == 'filermst'):
                    columns = columns.replace('zip_code,', ' TRIM(zip_code) as zip_code,')

                sqlqueryinput = sqlqueryinput.replace("*", columns)

                pgpool.apply_async(download_postgres_query_data, args=(properties, tableName, columns, sqlqueryinput))

            pgpool.close()
            pgpool.join()
            logger.info("\n\n     ******* Completed - Data download using SQL Query Files for Postgres *********** ")

        logger.info("\n")
        logger.info("\n******* Completed - Download Data from DB to DAT files *********** ")
        logger.info("\n")


    except SystemError as e:
        print(e)