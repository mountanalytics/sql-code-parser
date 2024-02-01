import pandas as pd
import textwrap
import sqlparse
from itertools import groupby
import re
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

def source_blocks_tracker(sql_list):
    blocks = []
    from_statement = "from "
    select_statement = "select "
    for element in sql_list:
        if select_statement in element[1].lower():
            blocks.append(element[0])
            o = sql_list.index(element)
            for i in sql_list[o:]:
                if from_statement in i[1].lower():
                    if len(i[1]) >5:
                        blocks.append(i[0])
                        break
                    else:
                        blocks.append(i[0]+1)
                        break
    
    #Searching for Insert Into Blocks
    insert_blocks = []
    insert_statement = 'insert into'
    for element in sql_list:
        if insert_statement in element[1].lower():
            insert_blocks.append(element[0])
    insert_blocks_full = []
    for element in sql_list:
        for i in insert_blocks:
            if i == element[0]:
                insert_blocks_full.append((i, element[1]))

    insert_prep = []
    for element in insert_blocks_full:
        matches = re.findall(r'\((.*?)\)', element[1])
        table_name = element[1].split("(")[0].strip("INSERT INTO ")
        columns_name = []
        for i in matches:
            l = i.split(",")
            for k in l:
                columns_name.append(k.strip(" "))
        target = []
        for column in columns_name:
            target.append(table_name +"." +column)
    #    target_alt = []
    #    for j in target:
    #        target_alt.append((element[0], "INSERT INTO", None, None, None, j))
    #alt_df = pd.DataFrame(target_alt, columns = ["Row Number", "Block", "Data Source", "Static", "Alias", "Target"])
        insert_prep.append((element[0], "INSERT INTO",None,None, None, target, None,None ))
    insert_df = pd.DataFrame(insert_prep, columns = ["Row Number", "Block", "Data Source", "Static", "Alias", "Target", "TF", "TF Type"])

    #Extracting Group By Blocks
    w_gb_blocks = []
    w_gb_content = []
    groupby_where = []
    w_gb = [ 'group by ']
    for element in sql_list:
        for i in w_gb:
            if i in element[1].lower():
                w_gb_blocks.append((element[0]-1, element[0], element[0]+1))
                groupby_where.append(element[0])
                groupby_where.append(element[0]+1)
    for element in sql_list:
        for l in w_gb_blocks:
            if l[0] == element[0]:
                w_gb_content.append([element[1]])
            if l[1] == element[0]:
                w_gb_content.append([l[0], element[1]])
            if l[2] == element[0]:
                w_gb_content.append([element[1]])
    unpack = []
    for element in w_gb_content:
        for i in element:
            unpack.append(i)
    w_gb_tuples = [tuple(unpack[i:i+4]) for i in range(0, len(unpack), 4)]

    gb_prep = []
    for expression in w_gb_tuples:
        criteria = expression[3].strip(";")
        table = expression[0].split(" ")[0]+expression[0].split(" ")[1]
        gb_prep.append(((expression[1]-1), "GROUP BY", None, None, None, None, None, None))
        gb_prep.append((expression[1], "GROUP BY", table+"."+criteria,None, None, None, None))
    gb_df = pd.DataFrame(gb_prep, columns = ["Row Number", "Block", "Data Source", "Static", "Alias", "Target", "TF", "TF Type"])

    #Extracting Where Blocks
    where_blocks = []
    where_statement = "where "
    for element in sql_list:
        if where_statement in element[1].lower():
            if "FROM " in sql_list[sql_list.index(element)-7][1]:
                where_blocks.append((sql_list[sql_list.index(element)-6][0],element[0], element[0]+1))
            else:
                where_blocks.append((element[0]-1, element[0], element[0]+1))
    where_content = []
    for element in sql_list:
        for l in where_blocks:
            if l[0] == element[0]:
                where_content.append([element[1].split(" b")[0].split(" ")[-1]])
            if l[1] == element[0]:
                where_content.append([l[1], element[1]])
            if l[2] == element[0]:
                where_content.append([element[1]])

    unpack2 = []
    for element in where_content:
        for i in element:
            unpack2.append(i)
    where_tuples = [tuple(unpack2[i:i+4]) for i in range(0, len(unpack2), 4)]
    where_prep =[]
    for element in where_tuples:
        on = element[0]+ "." + element[3].split(")")[0].split("(")[1].strip("b.")
        static = element[3].split(";")[-2].split(" ")[-1]
        tf = element[3].strip(";")
        where_prep.append((element[1],"WHERE", None, None, None, None, None, None))
        where_prep.append((element[1]+1,"WHERE", on, static, None, None, tf, ["YEAR", "EQUALS"]))
    where_df = pd.DataFrame(where_prep, columns = ["Row Number", "Block", "Data Source", "Static", "Alias", "Target", "TF", "TF Type"] )


    #Serach for Join Blocks in the code

    join_statements = ["inner join", "left join", "outer join", "right join"]
    on_statement = "on "

    join_blocks =[]
    for element in sql_list:
        for i in join_statements:
            if i in element[1].lower():
                join_blocks.append(element[0])
                p =  sql_list.index(element)
                for l in sql_list[p:]:
                    if on_statement in l[1].lower():
                        join_blocks.append(l[0])
                        break

                        
    tuples_blocks = [(blocks[i], blocks[i + 1]) for i in range(0, len(blocks), 2) if i + 1 < len(blocks)]
    join_tuples = [(join_blocks[i], join_blocks[i + 1]) for i in range(0, len(join_blocks), 2) if i + 1 < len(join_blocks)]

    join_content = []
    for element in sql_list:
        for i in join_tuples:
            if i[0] ==element[0]:
                join_content.append((element[0], element[1]))
            if i[1] == element [0]:
                join_content.append((element[0], element[1]))
    unpack3 = []
    for o in join_content:
        for u in o:
            unpack3.append(u)


    table_info = []
    for i in sql_list:
        for element in tuples_blocks:
            if i[0] == element[1]:
                if len(i[1].split(" "))> 3:
                    table_info.append((i[0], (i[1].split(" ")[0]+ " " +i[1].split(" ")[1]), i[1].split(" ")[2]))
                else:
                    table_info.append((i[0],i[1].strip("FROM "), None))

    subqueries= []
    for i in tuples_blocks:
        temporary =[]
        subqueries.append(i[1])
        w = i[0]
        while w < i[1]:
            temporary.append(sql_list[w-1][1])
            w = w+1
        subqueries.append(temporary)        
    
    tuple_subqueries = [(subqueries[i], subqueries[i + 1]) for i in range(0, len(subqueries), 2) if i + 1 < len(subqueries)]
                
    cleaned_subqueries =[]
    for i in tuple_subqueries:
        l =[]
        l.append(i[0])
        l.append([t.strip('\t') for t in i[1]])
        temp = [(l[i], l[i + 1]) for i in range(0, len(l), 2) if i + 1 < len(l)]
        cleaned_subqueries.append(temp)
    
    subqueries_final = []
    for i in cleaned_subqueries:
        for j in i:
            subqueries_final.append(j)

    blocks_final = []
    for i in tuples_blocks:
        blocks_final.append((i[1],i ))

    select_blocks = []
    for element in table_info:
        for i in subqueries_final:
            if element[0] == i[0]:
                u =element[0]
                w = element[1]
                t = i[1]
                for l in tuples_blocks:
                    if i[0] == l[1]:
                        p = l[0]-1
                        for q in sql_list:
                            if p == q[0]:
                                k = q[1]
                                select_blocks.append((u ,w ,t, p,k))

    #Look for rows with a Select opening and a From closing

    select_prep = []
    for element in select_blocks:
        row_number = element[0]-1
        statement = "SELECT"
        table = element[1].strip(";")
        for l in element[2]:
            k = l.split(" ")[1:]
            sources = []
            for y in k:
                y.split(",")[0]
                sources.append(y)
            combined_sources = [s.rstrip(",") for s in sources]
            columns_final = [table +"." + e for e in combined_sources]

        target = []
        target_table = element[-1].split("(")[0].strip("INSERT INTO ")
        target_columns = element[-1].split(")")[0].split("(")[1].split(" ")
        target_columns_last = [x.rstrip(",") for x in target_columns]
        target_pairs = [target_table+"."+t for t in target_columns_last]
         

    insert_blocks_fordf = []
    for element in sql_list:
        statement_link = "from  "
        statement_link2 = "insert into"
        if statement_link in element[1]:
            t = element[0]
        if statement_link2 in element[1]:
            y = element[1]
        insert_blocks_fordf.append((t,y))

    from_blocks = []
    from_statement = "from "
    for element in sql_list:
        if from_statement in element[1].lower():
            if len(element[1])< 6:
                from_blocks.append((element[0]+1, element[1]))
            else:
                from_blocks.append((element[0], element[1]))

    from_input = []
    
    for element in range(len(from_blocks)):
        from_input.append((from_blocks[element][0], insert_blocks_full[element][1]))
    cleaned_insert = [(i[0], i[1].strip("INSERT INTO ")) for i in from_input]
    target_table = []
    for y in cleaned_insert:
        u = y[1].split("(")[0].strip(" ")
        target_table.append((y[0],u))
    target_columns_df = []
    for element in cleaned_insert:
        d  = element[1].split("(")[-1].split(")")[0]
        parts = d.split(" ")
        cleaned_parts = [o.strip(",") for o in parts]
        target_columns_df.append((element[0],cleaned_parts))

    target_table_series = pd.Series([t[1] for t in target_table], index=[t[0] for t in target_table])
    target_columns_series = pd.Series([t[1] for t in target_columns_df], index=[t[0] for t in target_columns_df])

    #Converting everything to a dataframe
    df = pd.DataFrame([t[1:] for t in table_info], columns=['Table', 'Alias'], index=[t[0] for t in table_info])
    series_subqueries = pd.Series([t[1] for t in subqueries_final], index=[t[0] for t in subqueries_final])
    series_input = pd.Series([t[1] for t in cleaned_insert], index=[t[0] for t in cleaned_insert])


    empty_df = pd.DataFrame(index=range(1, 301), columns=['Column1'])
    empty_df = empty_df.rename_axis('Line Number')

    df = df.rename_axis("Line Number")

   

    #merged_df = empty_df.merge(df, left_index = True, right_index = True, how='outer')
    df["Table"] = df["Table"].str.rstrip(';')
    df["Arguments"] = series_subqueries
    df["Arguments"] = df["Arguments"].apply(remove_trailing)
    df["Target"] = series_input
    df["Target Table"]= target_table_series
    df["Target Columns"] = target_columns_series
    df["Block"] = pd.Series(dict(blocks_final))
    



    column_tuple = [(index, row["Arguments"]) for index, row in df.iterrows()]
    sources_column=[]
    for values in column_tuple:
        if values[1][0].lower() == "select":
           sources_column.append((values[0], values[1][1:]))
        else:
            splitted = values[1][0].split(" ")[1:]
            splitted_cleaned = [i.strip(",") for i in splitted]
            sources_column.append((values[0],splitted_cleaned ))

    sources_series = pd.Series([t[1] for t in sources_column], index=[t[0] for t in sources_column])
    df["Sources"] = sources_series

    source_alias = []
    for element in sources_column:
        list_aliases = []
        for i in element[1]:
            if " as " in i:
                m = i.split(" ")[-1]
                list_aliases.append(m)
            else:
                list_aliases.append(None)

        source_alias.append((element[0], list_aliases))
    source_alias_series = pd.Series([t[1] for t in source_alias], index=[t[0] for t in source_alias])

    df["Source Alias"] = source_alias_series


    from_blocks_prep = []
    for element in table_info:
        placeholder_statement = "FROM"
        from_blocks_prep.append((element[0], placeholder_statement, element[1].strip(";"), None, element[2], None, None, None))
    from_df = pd.DataFrame(from_blocks_prep, columns = ["Row Number", "Block", "Data Source", "Static", "Alias", "Target", "TF", "TF Type"] )


    target_tuples = [(index, row["Target Table"]) for index, row in df.iterrows()]
    target_columns_tuples = [(index, row["Target Columns"]) for index, row in df.iterrows()]
    select_prep2 = []
    complete_target = []
    for element in target_columns_tuples:
        for i in element[1]:
            for l in target_tuples:
                if element[0] == l[0]:
                    complete_target.append((element[0], l[1]+"."+ i.strip("\t")))

    #df['Source_Tables'] = df.apply(lambda row: combine_strings(row['Table'], row['Sources']), axis=1)

    df_new = pd.DataFrame(complete_target, columns=['Line Number', 'Target'],)



    result = pd.concat([insert_df, gb_df, where_df, from_df], axis=0)
    result = result.sort_values(by = "Row Number")
    blocks_all = blocks + insert_blocks + join_blocks +groupby_where
    sorted_blocks_all = sorted(blocks_all)


    df.drop(columns = ["Source Alias"], inplace = True)
    df.drop(columns = ["Block"], inplace = True)
    df.drop(columns = ["Alias"], inplace = True)
    df.drop(columns = ["Arguments"], inplace = True)
    #df.drop(columns = ["Target Columns"], inplace = True)
    df.drop(columns = ["Target"], inplace = True)



    #df['Source_Tables'] = df.apply(lambda row: combine_strings(row['Table'], row['Sources']), axis=1)
    #df.drop(columns = ["Table"], inplace = True)


    table_tuples = [(index, row["Table"]) for index, row in df.iterrows()]
    sources_tuples = [(index, row["Sources"]) for index, row in df.iterrows()]
    df.drop(columns = ['Target Table', "Target Columns"], inplace= True)

    paired_tuples =[]
    for tables in table_tuples:
        for element in sources_tuples:
            for i in element[1]:
                if element[0]==tables[0]:
                    paired_tuples.append((tables[0],tables[1]+"." +i ))


    complete_source_series = pd.DataFrame(paired_tuples, columns = ['Line Number', 'Source'])
    df_new['Source'] = complete_source_series['Source']
    df_new = df_new[["Line Number", "Source", "Target"]]
    #df_new.drop(columns= ["Line Number"], inplace= True)


    result.drop(columns = ['TF', 'TF Type', 'Alias', 'Static','Target'], inplace =True)
    return df_new








b= sql_to_string("Trial.sql")
a = source_blocks_tracker(b)

print(a)

#print(source)

#a.to_csv("Output.csv", index=False)