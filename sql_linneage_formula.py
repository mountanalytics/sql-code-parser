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

def sqllineage(sqlFile):
    sql_parse_obj = LineageRunner(sqlFile,dialect='ansi')
    src_tables = sql_parse_obj.source_tables
    src_cols = sql_parse_obj.get_column_lineage()
    src_cols_sub = sub_query(src_cols)
    return src_cols,src_tables, src_cols_sub

def Split_formula(sqlFile,src_cols):
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

def convert(src_cols):
    string_col = []
    for i in range(len(src_cols)):
        string_col.append(str(src_cols[i]))
    return string_col

def string_similarity(string1, string2):
    distance = Levenshtein.distance(string1, string2)
    max_length = max(len(string1), len(string2))
    similarity_ratio = 1 - (distance / max_length)
    return similarity_ratio

def find_differences(string1, string2):
    differ = difflib.Differ()
    diff = list(differ.compare(string1.split(), string2.split()))

    differing_words_1 = [item.split(None, 1)[-1] for item in diff if item.startswith('- ')]
    differing_words_2 = [item.split(None, 1)[-1] for item in diff if item.startswith('+ ')]

    return differing_words_1, differing_words_2

def match_strings(list1, list2, threshold):
    matches = []

    for str1 in list1:
        for str2 in list2:
            similarity = string_similarity(str1, str2)
            if similarity >= threshold:
                differences = find_differences(str1, str2)
                matches.append((str1, str2, similarity, differences))

    return matches
fd = open('Example3.sql', 'r',encoding='utf-8')
sqlFile = fd.read()
fd2 = open('Example3.sql', 'r',encoding='utf-8')
sqlFile2 = fd2.read()
#def general(sqlFile, sqlFile2):
src_cols,src_tables,src_cols_sub = sqllineage(sqlFile)
formulas, formulas_without_col, formula_amount = Split_formula(sqlFile,src_cols)
string_col = convert(src_cols)
string_col_sub = convert(src_cols_sub)
string_table = convert(src_tables)
string_formula = convert(formulas)
formulas_without_col = convert(formulas_without_col)
  
    
src_cols2,src_tables2,src_cols2_sub = sqllineage(sqlFile2)
formulas2,formulas_without_col2, formula_amount2 = Split_formula(sqlFile2,src_cols2)
string_col2 = convert(src_cols2)
string_col2_sub = convert(src_cols2_sub)
string_table2 = convert(src_tables2)
string_formula2 = convert(formulas2)
formulas_without_col2 = convert(formulas_without_col2)
    #---------------------------------------------- compare -------------------------------------------------------
threshold_value_cols = 0.95
threshold_value_formula = 0.95
matching_cols = match_strings(string_col, string_col2, threshold_value_cols)
Matching_cols_subquery = match_strings(string_col_sub, string_col2_sub, threshold_value_cols)
matching_tables = match_strings(string_table, string_table2, threshold_value_cols)
matching_formulas = match_strings(string_formula, string_formula2, threshold_value_formula)
matching_formulas_without_col = match_strings(formulas_without_col, formulas_without_col2, threshold_value_formula)
    
    
per_match_cols = len(matching_cols) / min(len(src_cols),len(src_cols2))
per_match_cols_subquery = len(Matching_cols_subquery) / min(len(src_cols),len(src_cols2))
per_match_tables = len(matching_tables) / min(len(src_tables),len(src_tables2))
per_match_for_with = len(matching_formulas) / min(formula_amount,formula_amount2)
per_match_for_without = len(matching_formulas_without_col)/ min(formula_amount,formula_amount2)
#    return per_match_cols, per_match_tables, per_match_for_with, per_match_for_without


#fd = open('Example3.sql', 'r',encoding='utf-8')
#sqlFile = fd.read()
#fd2 = open('Example2.sql', 'r',encoding='utf-8')
#sqlFile2 = fd2.read()
#per_match_cols, per_match_tables, per_match_for_with, per_match_for_without = general(sqlFile, sqlFile2)

per_match_overall_without = (1/3) * per_match_cols + (1/3) * per_match_tables + (1/3) * per_match_for_without
per_match_overall_with = (1/3) * per_match_cols + (1/3) * per_match_tables + (1/3) * per_match_for_with
per_match_overall_without_subquery = (1/3) * per_match_cols_subquery + (1/3) * per_match_tables + (1/3) * per_match_for_without
per_match_overall_with_subquery = (1/3) * per_match_cols_subquery + (1/3) * per_match_tables + (1/3) * per_match_for_with
if per_match_overall_without == per_match_overall_with and per_match_overall_without == 1:
    print('SQL Code is exactly the same')
if per_match_overall_without != per_match_overall_with and per_match_overall_without == 1:
    print('SQL Code has the same functionality but different syntax')
    
            












        