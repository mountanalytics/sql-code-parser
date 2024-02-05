#SQL separation trial
import pandas as pd
import textwrap
import sqlparse
from itertools import groupby
import re


df_tf = pd.read_excel('TransformationsSQL.xlsx')
source_yn = df_tf['Source_Syntax'].tolist() #list of function formulas

comparison_df = list(zip(df_tf['Source_Syntax'], df_tf['Transformation'])) #mapping table for assigning transformations
new_comparison = [(t[0].lower(), t[1]) for t in comparison_df] #Shifting the strings in the first elements to all lowercase for easier matching


#Extract Functions for searching from the Excel table of transformations
tree = []
for i in source_yn:
    k = i.split('[')
    tree.append(k[0])
pre = list(zip(tree, df_tf["Transformation"]))
second = [x for x in pre if x[0] not in ["", "'"]]
lowercase_second = [(t[0].lower(), t[1]) for t in second]

## Extract Operators and specific transformations from the Excel Table
expression_list = source_yn
isolated = []
for i in expression_list:
    pattern = re.compile(r'\[.*?\]\s*(.*?)\s*\[.*?\]')
    match = re.search(pattern, i)
    extracted_text = match.group(1) if match else None
    isolated.append(extracted_text)
new = [x.lower() for x in isolated if x not in [None, ",", ""]]
operators = list(set(new))
operators_final = [x + " " for x in operators]

twelve = []
for units in operators_final:
    for element in new_comparison:
      if units in element[0]:
           twelve.append((units, element[1]))

unique_dict = {}
for item in twelve:
    first_value = item[0]
    if first_value not in unique_dict:
        unique_dict[first_value] = item


unique_list = list(unique_dict.values())
unique_list[unique_list.index(("as ", 'CAST'))] = ('as ', "AS")  #Harcoded chnage to "as" and "in" operators
unique_list[unique_list.index(("in ", 'IN'))] = (' in ', "IN")

## Function to extract Functions from the Excel table
def function_splitter(alist):
    keywords = []
    for i in alist:
        k = i.split('[')
        keywords.append(k[0])
    functions = [x for x in keywords if x not in ["", "'"]]
    
    return functions


def sql_to_string(sqlfile):   #converts a sql file into a single string and formats it into line by line SQL syntax
    with open(sqlfile, 'r', encoding = 'utf-8') as f_in:
        lines = f_in.read()    
        query_string = textwrap.dedent("""{}""".format(lines))
        return query_string

def filter_transformations(input_string, filter_list): #Create a df with rows from the sql file and find the specific transformations in each line

    line_list = input_string.strip().split('\n')
    subsets = [(i, j.lower()) for i, j in enumerate(line_list, start=1) if not j.startswith('--')]
    function_list = [string.lower() for string in filter_list]

    search = []
    for string in function_list:
        for element in subsets:
            if string in element[1]:
                search.append(element)
    cleaned_list = sorted(list(set(search)), key=lambda x: x[0]) #list of rows with transfromations from the sql code
    
    ##Look for functions in the rows
    final_column=[]
    for element in lowercase_second:
        for i in cleaned_list:
            if element[0] in i[1]:
                final_column.append((i[0], element[1]))
    cleaned_final_column = sorted(list(set(final_column)), key=lambda x: x[0])
    grouped_tuples = groupby(cleaned_final_column, key=lambda x: x[0])
    extra_list =  [(key, [item[1] for item in group]) for key, group in grouped_tuples]
    
    ##Look for operators in the rows
    search2 = []
    for string in operators_final:
            for element in subsets:
                if string in element[1]:
                    search2.append(element)
    operator_listed = sorted(list(set(search2)), key=lambda x: x[0])

    operators_column = []
    for string in unique_list:
        for element in operator_listed:
            if string[0] in element[1]:
                operators_column.append((element[0], string[1]))
    cleaned_ops_column = sorted(list(set(operators_column)), key=lambda x: x[0])
    grouped_tuples2 = groupby(cleaned_ops_column, key=lambda x: x[0])
    extra_list2 = [(key, [item[1] for item in group]) for key, group in grouped_tuples2]

    #Convert lists to tuples
    final_column_series = pd.Series({t[0]: t[1] for t in extra_list})
    third_column_series = pd.Series({t[0]: t[1] for t in cleaned_list})
    operators_column_series = pd.Series({t[0]: t[1] for t in extra_list2})

    #convert eveything into df
    columns = ['Line_Number', 'Line']
    df = pd.DataFrame(subsets, columns=columns)
    df.set_index('Line_Number', inplace=True)
    df['TF_Line'] = third_column_series
    df['Functions'] = final_column_series
    df['Operators'] = operators_column_series
    df = df.drop("TF_Line", axis =1)
    return df


cleaned_functions  = function_splitter(source_yn)
sqlfile = 'northwind_db/Order Details_1996.sql'
trial = sql_to_string(sqlfile)
trial2 = filter_transformations(trial,cleaned_functions)
print(trial2)
#print(trial)

#print(cleaned_functions)