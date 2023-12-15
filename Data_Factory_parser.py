import json

def Strip_script(Input_string,Scriptlines):
    for i in range(len(Scriptlines)):
        Scriptlines[i] = Scriptlines[i].lstrip()
    for index, string in enumerate(Scriptlines):
        substring_index = string.find(Input_string)
        if substring_index != -1:
            index_begin_map = index
    
    for index, string in enumerate(Scriptlines):
        substring_index = string.find(")")
        if substring_index != -1 and index > index_begin_map:
            index_end_map = index
            break
    subset_list = Scriptlines[index_begin_map+1:index_end_map]
    for i in range(len(subset_list)):
        subset_list[i] = subset_list[i].rstrip(',')
    return subset_list
 
# some JSON:
json_file_path = 'MER_dataflow.json'

# Open the JSON file
with open(json_file_path, 'r') as json_file:
    data = json.load(json_file)
    
Scriptlines = data['properties']['typeProperties']['scriptLines']

Mapping =  Strip_script("mapColumn(",Scriptlines)
Input_table =  Strip_script("source(output(",Scriptlines)
for i in range(len(Input_table)):
    index_of_as = Input_table[i].find("as")
    
    if index_of_as != -1:
        # Extract the substring before "as"
        Input_table[i] = Input_table[i][:index_of_as].strip()
direct_lineage = []
index_direct = []
index_trans = []
trasformation_lineage = []
for i in range(len(Mapping)):
    for j in range(len(Input_table)):
        if Input_table[j] in Mapping[i]:
            direct_lineage.append(Mapping[i])
            index_direct.append(i)
for i in range(len(Mapping)):
    if i not in index_direct:
        index_trans.append(i)
        
trasformation_lineage = [row for idx, row in enumerate(Mapping) if idx not in index_direct]

for i in range(len(direct_lineage)):
    index_of_as = direct_lineage[i].find("=")
    
    if index_of_as != -1:
        # Extract the substring before "as"
        direct_lineage[i] = direct_lineage[i][:index_of_as].strip()

for i in range(len(Input_table)):
    Input_table[i] = Input_table[i].replace('{','').replace('}','')
direct_lineage = list(zip(direct_lineage, Input_table))

output_table = []
trans_table = []
for i in range(len(trasformation_lineage)) :
    split_parts = trasformation_lineage[i].split('=')
    
    # Remove leading and trailing whitespaces from each part
    output_table.append(split_parts[0].strip())
    trans_table.append(split_parts[1].strip())
full_trans = []
transformations = []
for i in range(len(trans_table)):
    for j in range(len(Scriptlines)):
        if trans_table[i] + ' =' in Scriptlines[j]:
            
            # Find the index of "=" in the line
            equal_index = Scriptlines[j].index('=')
            # Check if "~>" is present in the line
            if '~>' in Scriptlines[j]:
                tilde_index = Scriptlines[j].index('~>')
                
                # Extract the substring between "=" and "~" and remove leading/trailing whitespaces
                transformation_value = Scriptlines[j][equal_index + 1:tilde_index].strip()
            else:
                # If "~>" is not present, extract the substring after "=" and remove leading/trailing whitespaces
                transformation_value = Scriptlines[j][equal_index + 1:].strip()
            
            # Append the transformation_value to the transformations list
            transformations.append(transformation_value.rstrip(','))
            full_trans.append(trans_table[i] + ' = ' + transformation_value.rstrip(','))

input_lineage = []

for i in range(len(full_trans)):
    split_parts = full_trans[i].split('=')
    trans_name = split_parts[0].strip()
    trans = split_parts[1].strip()
    for j in range(len(transformations)):
        if trans_name in transformations[j]:
            # Replace the part of the string with the trans value
            transformations[j] = transformations[j].replace(trans_name, f'({trans})')

for idx, transformation in enumerate(transformations):
    for input_value in trans_table:
        if input_value in transformation:
            input_lineage.append({"input_value": input_value, "row_index": idx})   
        
for idx, transformation in enumerate(transformations):
    for input_value in Input_table:
        if input_value in transformation:
            input_lineage.append({"input_value": input_value, "row_index": idx})

combined_lineage = {}
for entry in input_lineage:
    row_index = entry["row_index"]
    if row_index not in combined_lineage:
        combined_lineage[row_index] = []
    combined_lineage[row_index].append(entry["input_value"])

output_table = list(zip(output_table, transformations, combined_lineage.values()))

#combine output table and direct_lineage where we have the input columns, output columns, and transformations. Linked to the Mapping table.



