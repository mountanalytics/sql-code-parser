from sqllineage.runner import LineageRunner
import re
import numpy as np
import copy
import Levenshtein
import difflib

def sub_query(src_cols):
    src_cols_dub = []

    for i in range(len(src_cols)):
        src_cols_dub.append([])  # Initialize an empty list for each row in src_cols_dub
        
        for j in range(len(src_cols[i])):
            if 'SubQuery' not in str(src_cols[i][j].parent_candidates) or j == 0:
                src_cols_dub[i].append(src_cols[i][j])
    return src_cols_dub 

def sqllineage(sqlFile):    #Function for the sqllineage runner it will return the tables and columns in the SQL code
    sql_parse_obj = LineageRunner(sqlFile,dialect='ansi')
    src_tables = sql_parse_obj.source_tables
    src_cols = sql_parse_obj.get_column_lineage()
    src_cols_sub = sub_query(src_cols)
    return src_cols,src_tables, src_cols_sub


def Split_formula(sqlFile,src_cols):    #Function for splitting the transformations with a +, -, *, / operator 
    tokenized_sql = sqlFile.split()
        #------------------------------------------------- Find formulas in SQL code ------------------------------------------
        # Find all indices of operators: '+', '-', '*', '/'
    operator_indices = [index for index, word in enumerate(tokenized_sql) if any(op in word for op in ['+', '-', '*', '/']) and not word.startswith('--')]
       # Find all indices where .* appears in the tokenized SQL
    operator_indices_dot = [index for index, word in enumerate(tokenized_sql) if re.search(r'\.\*', word)]
        
        # Exclude indices with .* from the list of operator indices
    operator_indices = [index for index in operator_indices if index not in operator_indices_dot]
        # Create a subset of indices within 2 of each other
        
        # Sort the indices
    subset_indices = np.sort(operator_indices)
        
        # Initialize a list to store groups of indices
    index_groups = []
        
        # Iterate through the indices and group them
    current_group = [subset_indices[0]]
    for index in subset_indices[1:]:
        if index - current_group[-1] <= 2:
            current_group.append(index)
        else:
            index_groups.append(current_group)
            current_group = [index]
        
        # Add the last group
    index_groups.append(current_group)
    formulas = []
    for i in range(len(index_groups)):
        start_index = min(index_groups[i])-1
        end_index = max(index_groups[i])+2
        formulas.append(tokenized_sql[start_index:end_index])
        if tokenized_sql[end_index] == 'AS':
            formulas[i].append('=')
            formulas[i].append(tokenized_sql[end_index+1])
        elif tokenized_sql[start_index-1] == '=':
            formulas[i].append('=')
            formulas[i].append(tokenized_sql[start_index-2])
    for i in range(len(formulas)):
        for j in range(len(formulas[i])):
            if str(type(formulas[i][j])) == "<class 'str'>":
                formulas[i][j] = formulas[i][j].replace(",", "").replace(";", "").replace(")", "").replace("(", "")
    formula_amount = len(formulas)
    
    for i in range(len(formulas)):
        compare = formulas[i][-1]
        for k in range(len(formulas)):
            for j in range(len(formulas[k])-1):
                if formulas[k][j] == compare:
                    new_formula = formulas[k].copy()
                    formulas.append(new_formula)
                    formulas[-1].pop(j)
                    formulas[-1][j:j] = formulas[i][:-2]
    
    temp = [] 
    formulas_copy = copy.deepcopy(formulas)
    formula_string = copy.deepcopy(formulas)
    for i in range(len(formulas_copy)):
        for j in range(len(src_cols)):
            if src_cols[j][1].raw_name in formulas_copy[i][-1]:
               temp.append(src_cols[j][1])
               break
                
    for j in range(len(formulas)):
        for i in range(len(src_cols)):
           for k in range(len(formulas[j])):
                if src_cols[i][0].parent != None:
                    if src_cols[i][0].parent_candidates[0].alias+'.'+src_cols[i][0].raw_name in formulas[j][k]:
                        formulas_copy[j].append(src_cols[i][0])
                elif src_cols[i][0].parent == None:
                    if src_cols[i][0].raw_name in formulas[j][k]:
                        formulas_copy[j].append(src_cols[i][0])
                 
    for i in range(len(formulas_copy)):
        for j in range(len(formulas_copy[i])):
            if str(type(formulas_copy[i][j])) == "<class 'sqllineage.core.models.Column'>":
                compare = formulas_copy[i][j].parent_candidates[0].alias + '.' + formulas_copy[i][j].raw_name
                for k in range(j, len(formulas_copy[i])):
                    if str(type(formulas_copy[i][k])) == "<class 'sqllineage.core.models.Column'>":
                        if formulas_copy[i][k].parent_candidates[0].alias + '.' + formulas_copy[i][k].raw_name == compare:
                            unique_strings = set(map(str, formulas[i]))
                            if str(formulas_copy[i][j]) not in unique_strings:
                                formulas[i].append(formulas_copy[i][j])
                if formulas_copy[i][j].parent == None:
                    formulas[i].append(formulas_copy[i][j])


    for i in range(len(temp)):
        formulas[i].append(temp[i])

    for i in range(len(formulas)):
        # Create a copy of the list to iterate over
        formulas_copy = formulas[i].copy()

        for j in range(len(formulas_copy)):
            compare = str(formulas_copy[j])

            for k in range(len(formulas_copy)):
                if str(formulas_copy[k]) == compare and j != k:
                    # Keep one of the duplicates and remove the others
                    formulas[i] = [x for x in formulas[i] if str(x) != compare or x == formulas_copy[j]]

        # If you want to keep the first occurrence (index 0) of each unique element, you can use a set
        seen = set()
        formulas[i] = [x for x in formulas[i] if str(x) not in seen and not seen.add(str(x))]
        
    return formulas, formula_string, formula_amount

# call the SQL file you want to parse
fd = open('Example.sql', 'r',encoding='utf-8')
sqlFile = fd.read()
#def general(sqlFile, sqlFile2):
src_cols,src_tables,src_cols_sub = sqllineage(sqlFile)
formulas, formulas_without_col, formula_amount = Split_formula(sqlFile,src_cols)









        