import json
import copy

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
Source1 = data['properties']['typeProperties']['sources'][0]['dataset']['referenceName']
Sink1 = data['properties']['typeProperties']['sinks'][0]['dataset']['referenceName']
for i in range(len(direct_lineage)):
    index_of_as = direct_lineage[i].find("=")
    
    if index_of_as != -1:
        # Extract the substring before "as"
        direct_lineage[i] = direct_lineage[i][:index_of_as].strip()
    direct_lineage[i] = Sink1 + '.' + direct_lineage[i]

for i in range(len(Input_table)):
    Input_table[i] = Input_table[i].replace('{','').replace('}','')

#------------------------- watch if multiple input tables -------------------------
Source_table = copy.deepcopy(Input_table)
for i in range(len(Source_table)):   
    Source_table[i] = Source1 + '.' + Input_table[i]
direct_lineage = list(zip(Source_table, direct_lineage))
#----------------------------------------------------------------------------------

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

# ------------------------- watch multiple input tables -----------------------
for i in range(len(transformations)):
    for j in range(len(Input_table)):
        if Input_table[j] in transformations[i]:
            transformations[i] = transformations[i].replace(Input_table[j],Source_table[j])
#----------------------------------------------------------------

combined_lineage = {}
for entry in input_lineage:
    row_index = entry["row_index"]
    if row_index not in combined_lineage:
        combined_lineage[row_index] = []
    combined_lineage[row_index].append(entry["input_value"])
# ------------------------- watch multiple input/output tables -----------------------
for i in range(len(output_table)):
    output_table[i] = Sink1 + '.' + output_table[i]
for i in range(len(combined_lineage)):
    for j in range(len(combined_lineage[i])):
        for k in range(len(Input_table)):
            if Input_table[k] == combined_lineage[i][j]:
                combined_lineage[i][j] = Source_table[k]
             
#-----------------------------------------------------------------------

lineage_table = list(zip(output_table, combined_lineage.values()))
output_table = list(zip(output_table, transformations, combined_lineage.values()))

com_lineage = [[] for _ in range(max(max(index_direct),max(index_trans))+1)]
for i in range(len(index_direct)):
    for j in range(len(com_lineage)):
        if index_direct[i] == j:
            com_lineage[j] = direct_lineage[i]
            break
for i in range(len(index_trans)):
    for j in range(len(com_lineage)):
        if index_trans[i] == j:
            com_lineage[j] = output_table[i]
            break

result_table_lineage = []
for table_name, columns in lineage_table:
    for column_name in columns:
        result_table_lineage .append((column_name,table_name))

result_table_lineage = direct_lineage + result_table_lineage

unique_values = list(set(item[0] for item in result_table_lineage))
unique_values2 = list(set(item[1] for item in result_table_lineage))
unique_values = unique_values + unique_values2

source_col, sink_col = zip(*result_table_lineage)

plot_source = []
plot_sink = []
for i in range(len(source_col)):
    for j in range(len(unique_values)):
        if source_col[i] == unique_values[j]:
            plot_source.append(j)
            break

for i in range(len(sink_col)):
    for j in range(len(unique_values)):
        if sink_col[i] == unique_values[j]:
            plot_sink.append(j)
            break

import plotly.graph_objects as go
from plotly.offline import plot

# Define the nodes and links
values = [1] * len(plot_source)

# Create the Sankey diagram
fig = go.Figure(data=[go.Sankey(
    node=dict(
        pad=100,
        thickness=20,
        #line=dict(color="blue", width=0.05),
        label=unique_values
    ),
    link=dict(
        arrowlen=22,
        source=plot_source,
        target=plot_sink,
        value=values,
    )
)])

# Show the plot
plot(fig, validate=False)






