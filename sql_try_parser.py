import pandas as pd
import textwrap
import sqlparse
from itertools import groupby
import re
import copy
import pypyodbc as odbc
import configparser
odbc.lowercase = False


#config = configparser.ConfigParser()
#config.read('azure_sql.ini')
#server = config["azure-sql"]["server"]
#database = config["azure-sql"]["database"]
#Password = config["azure-sql"]["password"]
#Username = config["azure-sql"]["username"]
#connection_string = 'Driver={ODBC Driver 18 for SQL Server};Server='+server+';Database='+database+';Encrypt=yes;UID='+Username+';PWD='+ Password
#conn = odbc.connect(connection_string)
#cursor = conn.cursor()
#sql_table = """
#SELECT Table_Name FROM INFORMATION_SCHEMA.TABLES
#"""
#cursor.execute(sql_table)
#datatable = cursor.fetchall()

#tables = [table[0] for table in datatable if table[0] != "database_firewall_rules"]
#columns = []
#for i in range(len(tables)):
#    sql_table = """SELECT Column_Name FROM INFORMATION_SCHEMA.COLUMNS Where Table_Name='"""+tables[i]+"'"
#    cursor.execute(sql_table)
#    datacolumn = cursor.fetchall()#
#
#
#    columns.append([column[0] for column in datacolumn])
#source = {}
#for i in range(len(tables)):
#    source[tables[i]] = columns[i]



#Returns "Source" a dictionary of the tables and column names wihtin them from the azure database


def combine_strings(single, string_list):
    return [single + ' ' + s for s in string_list]

def sql_to_string(sqlfile):   #converts a sql file into a list of tuples with the indices of the lines
    with open(sqlfile, 'r', encoding = 'utf-8') as f_in:
        lines = f_in.read()    
        query_string = textwrap.dedent("""{}""".format(lines))
        line_list = query_string.strip().split('\n')
        linear_list = [x.strip('\t') for x in line_list]
        return list(enumerate(linear_list, start=1))

def remove_trailing(lst):
    return [s.rstrip(', ') for s in lst]


block_lines = []
block_list = []
subquery = []
sql_list= sql_to_string("C:/Users/ErwinSiegers/Documents/GitHub/sql-code-parser/northwind_db/Order Details_1998.sql")
blocks = pd.read_excel(("C:/Users/ErwinSiegers/Documents/GitHub/sql-code-parser/blocks_SQL.xlsx"))

for i in range(len(sql_list)):
    for j in range(len(blocks['Function'])):
        if blocks['Function'][j] in sql_list[i][1].lower():
            block_lines.append((i,blocks['Function'][j].strip()))

for i in range(len(block_lines)):
    if block_lines[i][1] == 'inner join':
        for j in range(len(block_lines)):
            if block_lines[j][1] == 'on' and j>i and j != len(block_lines)-1:
                block_list.append((block_lines[i][0],block_lines[j+1][0]-1,block_lines[i][1]))
                break
            elif block_lines[j][1] == 'on' and j>i and j == len(block_lines)-1:
                block_list.append((block_lines[i][0],len(sql_list)-1,block_lines[i][1]))
                break
    if block_lines[i][1] == 'select':
        for j in range(len(block_lines)):
            if block_lines[j][1] == 'from' and j>i and j != len(block_lines)-1:
                block_list.append((block_lines[i][0],block_lines[j+1][0]-1,block_lines[i][1]))
                break
            elif block_lines[j][1] == 'from' and j>i and j == len(block_lines)-1:
                block_list.append((block_lines[i][0],len(sql_list)-1,block_lines[i][1]))
                break

for i in range(len(block_list)):
    if block_list[i][2] == 'select':
        from_line = block_list[i][0]
        to_line = block_list[i][1]
        for j in range(len(block_list)):
            if block_list[j][2] == 'inner join' and from_line > block_list[j][0] and to_line < block_list[j][1]: #add sth like distance from to and from block to get the correct subquery so could be subsubquery
                subquery.append((block_list[i],block_list[j]))
                break
sub_lines = []
for i in range(len(subquery)):
    temp = []
    for j in range(len(block_lines)):
        if block_lines[j][0] == subquery[i][1][0]:
            temp.append(block_lines[j])
        elif block_lines[j][0] == subquery[i][1][1]:
            temp.append(block_lines[j])
        elif block_lines[j][0] > subquery[i][1][0] and block_lines[j][0] < subquery[i][1][1]:
            temp.append(block_lines[j])
    sub_lines.append(copy.deepcopy(temp))

flattened_list = [item for sublist in sub_lines for item in sublist]
  
outer_blocks = []
for i in range(len(block_lines)):
    if block_lines[i] not in flattened_list:
        outer_blocks.append(block_lines[i])

all_sub = []
calcutable = []
for i in range(len(sub_lines)):
    all_lines = []
    temp = []
    temp2 = []
    calcutable = []
    for j in range(len(sub_lines[i])):
        temp = []
        temp2 = []
        calcu = []
        names = []
        alias = ''
        if sub_lines[i][j][1] == 'select':
            temp = []
            temp2 = []
            temp.extend(sql_list[sub_lines[i][j][0]+1:sub_lines[i][j+1][0]])
            for k in range(len(temp)):
                if ' as ' in temp[k][1]:
                    aa = temp[k][1].split(' as ')
                    calcu.append((k,aa[0]))
                    names.append((k,aa[1].replace(',', '')))
                else:
                    names.append((k,temp[k][1].replace(',', '')))
            calcutable.extend((names,calcu))
            all_lines.append((sub_lines[i][j],calcutable))
        if sub_lines[i][j][1] == 'from' or sub_lines[i][j][1] == 'group by':
            temp = []
            temp2 = []

            temp.extend(sql_list[sub_lines[i][j][0]+1:sub_lines[i][j+1][0]])
            for k in range(len(temp)):
                match = re.search(r'\) (.+)', temp[k][1])
                if match:
                    alias = match.group(1)
                else:
                    temp2.append(temp[k][1])
            for k in range(len(all_lines[1][1][0])):
                if alias != '':
                    new_value = alias +'.'+all_lines[1][1][0][k].strip()
                else:
                    new_value = all_lines[1][1][0][k][1].strip()
                new_list = list(all_lines[1][1][0])
                new_list[k] = new_value

                # Create a new list with the modified sub-list
                all_lines[1][1][0] = new_list
            all_lines.append((sub_lines[i][j],temp2))
        if sub_lines[i][j][1] == 'inner join':
            all_lines.append(sub_lines[i][j])
        if sub_lines[i][j][1] == 'on':
            on_temp = []
            for k in range(len(block_list)):
                if all_lines[0][0] == block_list[k][0]:
                    till_line = block_list[k][1]
            from_sub = sub_lines[i][j][0]
            if from_sub == till_line:
                match = re.search(r'[oO][nN] (.+)',sql_list[till_line][1])
                if match:
                    result = match.group(1)
                else:
                    print("No match found")
                on_temp.append(result)
            else:
                for k in range(from_sub,till_line+1):
                    on_temp.append(sql_list[k])
            all_lines.append((sub_lines[i][j],on_temp))
    all_sub.append(copy.deepcopy(all_lines))

all_outside = []
for i in range(len(outer_blocks)):
    
    if outer_blocks[i][1] == 'select':
        temp = []
        temp2 = []
        calcu = []
        calcutable = []
        temp.extend(sql_list[outer_blocks[i][0]+1:outer_blocks[i+1][0]])
        for k in range(len(temp)):
            if ' as ' in temp[k][1]:
                aa = temp[k][1].split(' as ')
                if len(aa) == 2:
                    calcu.append((k,aa[0]))
                    names.append((k,aa[1].replace(',', '')))
                else:
                    names.append((k,aa[-1].replace(',', '')))
                    temp_calcu = ''
                    for j in range(len(aa)):
                        if j != len(aa)-1:
                            if j == len(aa)-2:
                                temp_calcu = temp_calcu + aa[j].strip()
                            else:
                                temp_calcu = temp_calcu + aa[j].strip() + ' as '
                    calcu = (k,temp_calcu)
            else:
                names.append((k,temp[k][1].replace(',', '')))
        calcutable.extend((names,calcu))
        all_outside.append((outer_blocks[i],calcutable))















