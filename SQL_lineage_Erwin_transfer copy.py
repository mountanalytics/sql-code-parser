from sqlglot import parse_one, exp
from sqlglot.dialects.tsql import TSQL
import pypyodbc as odbc
odbc.lowercase = False
import configparser
import copy
from collections import defaultdict
from collections import OrderedDict 
import pandas as pd

def SQL_database_parser(server,database):  
    config = configparser.ConfigParser()
    
    config.read("C:/Users/MátéKaiser/OneDrive - Mount Analytics/Desktop/azure_sql.ini")
    #--------------------------------make something with pasword --------------------------------------
    Password = config["azure-sql"]["password"]
    Username = config["azure-sql"]["username"]
    connection_string = 'Driver={ODBC Driver 18 for SQL Server};Server='+server+';Database='+database+';Encrypt=yes;UID='+Username+';PWD='+ Password
    conn = odbc.connect(connection_string)
    cursor = conn.cursor()
    sql_table = """
    SELECT Table_Name FROM INFORMATION_SCHEMA.TABLES
    """
    cursor.execute(sql_table)
    datatable = cursor.fetchall()
    
    tables = [table[0] for table in datatable if table[0] != "database_firewall_rules"]
    columns = []
    for i in range(len(tables)):
        sql_table = """SELECT Column_Name FROM INFORMATION_SCHEMA.COLUMNS Where Table_Name='"""+tables[i]+"'"
        cursor.execute(sql_table)
        datacolumn = cursor.fetchall()
    
        columns.append([column[0] for column in datacolumn])
    table_columns = {}
    for i in range(len(tables)):
        table_columns[tables[i]] = columns[i]
    return table_columns




