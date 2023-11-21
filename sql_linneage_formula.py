from sqllineage.runner import LineageRunner
import re
import numpy as np
import copy

fd = open('Example2.sql', 'r',encoding='utf-8')
sqlFile = fd.read()
tokenized_sql = sqlFile.split()

sql_parse_obj = LineageRunner(sqlFile,dialect='ansi')
src_tables = sql_parse_obj.source_tables
src_cols = sql_parse_obj.get_column_lineage()

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
    formulas[i].append(tokenized_sql[end_index+1])
    
temp = [] 
temp2 = []
formulas_copy = copy.deepcopy(formulas)
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
            compare = formulas_copy[i][j].parent_candidates[0].alias+'.'+formulas_copy[i][j].raw_name
            for k in range(j,len(formulas_copy[i])):
                if str(type(formulas_copy[i][k])) == "<class 'sqllineage.core.models.Column'>":
                    if formulas_copy[i][k].parent_candidates[0].alias+'.'+formulas_copy[i][k].raw_name == compare:
                        if j != k:
                            formulas[i].append(formulas_copy[i][j])
            if formulas_copy[i][j].parent == None:
                formulas[i].append(formulas_copy[i][j])
for i in range(len(temp)):
    formulas[i].append(temp[i])
                        