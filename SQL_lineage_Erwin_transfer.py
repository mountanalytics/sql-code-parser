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

def transformer(node):
    sub_alias_di = list(sub_alias_dict.keys())
    for i in range(len(sub_alias_di)):
    
     #   if isinstance(node, exp.TableAlias) and node.indentifier == sub_alias_di[i]:
       #     return parse_one(sub_alias_dict[sub_alias_di[i]])
        if isinstance(node, exp.Identifier) and node.alias_or_name == sub_alias_di[i]:
            return parse_one(sub_alias_dict[sub_alias_di[i]])
    alias_di = list(Alias_dict.keys())
    for i in range(len(alias_di)):
    
    
        if isinstance(node, exp.Column) and node.table == alias_di[i]:
           return parse_one(Alias_dict[alias_di[i]] + "." + node.name)
           
    
    
    
    
    
    return node








class Graph: 
    def __init__(self) -> None: 
        self.adj_list = defaultdict(list) #dictionary containing adjacency list 
  
    def add_edge(self, u, v) -> None: 
        self.adj_list[u].append(v) 
  
    def topological_sort(self) -> list: 
        visited = set()
        reverse_topo = list() 
  
        vertices = set(self.adj_list.keys())
        for vertex in vertices: 
            if vertex not in visited:
                self._topological_sort_util(vertex, visited, reverse_topo) 
        return list(reversed(reverse_topo))
    
    def _topological_sort_util(self, vertex, visited: set, reverse_topo: list) -> None: 
        visited.add(vertex)
        for adj_vertex in self.adj_list[vertex]: 
            if adj_vertex not in visited: 
                self._topological_sort_util(adj_vertex, visited, reverse_topo) 
        reverse_topo.append(vertex)


def extractor_info(lookup_list,astb):
    general_syntax= []
    transformations = []
    scripts = []
    for i in lookup_list:    
        o = list(astb.find_all(getattr(exp, i)))
        for element in o:
            general_syntax.append(element.sql())
            scripts.append(repr(element))
            if len(o)>0:
                transformations.append(i)
    
    
    matched = list(zip(transformations, general_syntax))
    return matched
df_tf = pd.read_excel("C:/Users/MátéKaiser/OneDrive - Mount Analytics/Desktop/functions.xlsx")
lookup_list = list(df_tf["Parser Keyword"])
SQL_Connect = pd.read_excel("C:/Users/MátéKaiser/OneDrive - Mount Analytics/Desktop/SQL_Connect.xlsx")
query = """
SELECT 
	a.*, 
    b.*,
	c.NrOfProducts, 
	c.avg_order_unitprice,
	c.max_order_discount, 
	c.min_order_discount, 
	c.total_quantity,
	d.product_quantity_year, 
	round((cast(a.quantity as numeric (10,2))/cast(d.product_quantity_year as numeric (10,2)))*100,1) as perc_of_product_quantity_year
FROM 
	MA_NorthWindDB.[Order Details] a INNER JOIN MA_NorthWindDB.dbo.Orders b
	on a.OrderID = b.OrderID INNER JOIN MA_NorthWindDB.dbo.[Products] q
	on a.ProductID = q.ProductID
	INNER JOIN 
		(	
			select 
				orderid, 
				count(*) as NrOfProducts, 
				avg(unitprice) as avg_order_unitprice,
				max(Discount) as max_order_discount,  
				min(Discount) as min_order_discount, 
				sum(quantity) as total_quantity
			from 
				dbo.[Order Details] ode 
			group by 
				OrderID
		) c

	on a.OrderID = c.OrderID	
	INNER JOIN 
		(
			select 
				productID, 
				sum(quantity) as product_quantity_year
			from 
				dbo.[Order Details] ode 
			group by 
				productID
		) d
	on a.productID = d.productID
where 
	YEAR(b.OrderDate) = 1998;

 
"""
ast = parse_one(query, read="tsql")
#join = ast.args["joins"]
#froms = repr(ast.args["from"])
#leaf = ast.args["expressions"]

Anonymous = list(ast.find_all(exp.Anonymous))
table_alias = list(ast.find_all(exp.Table))
subqueries = list(ast.find_all(exp.Subquery))
list(ast.find_all(exp.Expression))
zsub = []
zalias = []

for i in range(len(subqueries)):
    zsub.append(repr(subqueries[i]))
for i in range(len(table_alias)):
    zalias.append(repr(table_alias[i]))


global Alias_dict
Alias_dict = {}
database_dict = {}
for i in range(len(table_alias)):
    temp = table_alias[i].args
    database_name = ""
    schema_name = ""
    table_name = ""
    alias = ""
    connection = ""
    for i in temp:
        if i == 'this' and str(type(temp[i])) != "<class 'NoneType'>":
            table_name = temp[i].alias_or_name
        elif i == 'db' and str(type(temp[i])) != "<class 'NoneType'>":
            schema_name = temp[i].alias_or_name
        elif i == 'catalog' and str(type(temp[i])) != "<class 'NoneType'>":
            database_name = temp[i].alias_or_name
        elif i == 'alias' and str(type(temp[i])) != "<class 'NoneType'>":
            alias = temp[i].alias_or_name
        if alias != "":
            all_database_keys = database_dict.keys()
            all_database_keys = list(all_database_keys)
            if ' ' in database_name:
                database_name = '"'+database_name+'"'
            if ' ' in schema_name:
                schema_name = '"'+schema_name+'"'
            if ' ' in table_name:
                table_name = '"'+table_name+'"'
            if database_name != "" and schema_name != "" and table_name != "":
                name = database_name + '.' + schema_name + '.' + table_name
            elif schema_name != "" and table_name != "":
                name = schema_name + '.' + table_name
            elif table_name != "":
                name = table_name
            if database_name not in all_database_keys and database_name != "":  
                for j in range(len(SQL_Connect)):
                    if database_name == SQL_Connect['Database'][j]:
                       connection = SQL_Connect['Connection'][j]
                       break
                database_dict.update({database_name:SQL_database_parser(connection,database_name)})
            
            Alias_dict.update({alias:name})
global sub_alias_dict
sub_alias_dict = {}
All_alias = list(ast.find_all(exp.TableAlias))
Subquery_nmbr = 1
for i in range(len(All_alias)):
    key_match = extractor_info(lookup_list,All_alias[i])
    if key_match[0][1] not in Alias_dict.keys():
        name = 'SUBQUERY_' + str(Subquery_nmbr)
        Alias_dict.update({key_match[0][1]:name})
        sub_alias_dict.update({key_match[0][1]:name})
        Subquery_nmbr += 1  

ast = ast.transform(transformer)  
a = repr(ast)
b = repr(parse_one(query, read="tsql"))
stars_dict = {}
stars = list(ast.find_all(exp.Star))
for i in range(len(stars)):
    if type(stars[i].parent) == exp.Column:
        database_key = stars[i].parent.catalog
        table_key = stars[i].parent.table
        if database_key != "":
            Columns = database_dict[database_key][table_key]
            name_key = stars[i].parent.catalog + '.' + stars[i].parent.db + '.' + stars[i].parent.table + '.*'
            stars_dict.update({name_key:Columns})
        else:
            dic_keys = list(database_dict.keys())
            for keys in dic_keys:
                whole_database = database_dict[keys]
                if table_key in list(whole_database.keys()):
                    Columns = database_dict[keys][table_key]
                    name_key = (stars[i].parent.catalog + '.' + stars[i].parent.db + '.' + stars[i].parent.table + '.*').lstrip('.')
                    if keys not in name_key:
                        name_key = keys + '.' + name_key
                    stars_dict.update({name_key:Columns})










match_sub_in = []
match_sub_out = []

for i in range(len(zsub)):
    for j in range(len(zsub)):
        if zsub[j].replace('  ', '') in zsub[i].replace('  ', '') and i != j:
            match_sub_in.append((j,i,len(zsub[j])/len(zsub[i])))

def make_pairs(match_sub_in):
    pairs = []
    unique_fits = list(set([tup[1] for tup in match_sub_in]))
    for i in range(len(unique_fits)):
        indices_of_zeros = [index for index, value in enumerate(match_sub_in) if value[1] == unique_fits[i]]
        match_per = 0
        match_temp = ()
        for j in range(len(indices_of_zeros)):
            if match_per < match_sub_in[indices_of_zeros[j]][2]: 
                match_temp = (match_sub_in[indices_of_zeros[j]][0],match_sub_in[indices_of_zeros[j]][1])           
                match_per = match_sub_in[indices_of_zeros[j]][2]
            if j == len(indices_of_zeros)-1 and len(match_temp) != 0:
                pairs.append(match_temp)
    return pairs
pairs = make_pairs(match_sub_in)
#pairs.append((5,6))
#pairs.append((6,7))
#pairs.append((8,9))


  


def order_tuples(zsub,pairs):
    g = Graph() 
    for edge in pairs:
        g.add_edge(edge[0], edge[1])
    
    vertices_topo_sorted = g.topological_sort() 
    edge_tuples = [(u, v) for u, v in zip(vertices_topo_sorted[0:], vertices_topo_sorted[1:])]
    # Group tuples based on whether they are in pairs or not
    grouped_tuples = []
    current_group = []
    
    for tup in edge_tuples:
        if tup in pairs:
            current_group.append(tup)
        elif current_group:
            grouped_tuples.append(current_group)
            current_group = []
    
    # Append the last group if not empty
    if current_group:
        grouped_tuples.append(current_group)
    return grouped_tuples
    
    
def flatten_tuples(grouped_tuples,zsub):
    sorted_grouped_tuples = []
    for i in range(len(grouped_tuples)):
        first = grouped_tuples[i][0][0]
        last = grouped_tuples[i][-1][-1]
        if len(zsub[first]) < len(zsub[last]):
            sorted_grouped_tuples.append(grouped_tuples[i])
        else:
            sorted_grouped_tuples.append(grouped_tuples[i][::-1])
    
    flattened_tuple = ()
    final_order_sub = []
    for j in range(len(sorted_grouped_tuples)):
        flattened_tuple = ()
        for tpl in sorted_grouped_tuples[j]:
            flattened_tuple += tpl
    
        
    
        # Remove duplicates while preserving order
        unique_tuple = tuple(OrderedDict.fromkeys(flattened_tuple))
        final_order_sub.append(unique_tuple)
    return final_order_sub
final_order_sub = flatten_tuples(order_tuples(zsub,pairs),zsub)








bla = []
for e in range(len(subqueries)):
    subq_select = list(subqueries[e].find_all(exp.Select))
    subq_select_form = extractor_info(lookup_list,subq_select[0])
    subq_from = list(subqueries[e].find_all(exp.From))
    subq_from_form = extractor_info(lookup_list,subq_from[0])
    subq_group = list(subqueries[e].find_all(exp.Group))
    subq_group_form = extractor_info(lookup_list,subq_group[0])
    subq_alias = list(subqueries[e].find_all(exp.TableAlias))
    subq_alias_form = extractor_info(lookup_list,subq_alias [0])
    
    subq_combine = subq_from_form + subq_group_form
    for item in subq_combine:
        if item in subq_select_form:
            subq_select_form.remove(item)
    
    for item in subq_select_form:
        if item in subq_alias_form:
            subq_alias_form.remove(item)
    
    couples_sub = []
    subq_select_form = list(set(subq_select_form))
    for i in range(len(subq_select_form)):
        for j in range(len(subq_select_form)):
            if subq_select_form[j][1] in subq_select_form[i][1] and i != j:
                couples_sub.append((j,i,len(subq_select_form[j][1])/len(subq_select_form[i][1])))
    second_elements = [item[1] for item in subq_select_form]
    pairs_sub = make_pairs(couples_sub)
    
    
    multi_lines = []
    for i in range(len(pairs_sub)):
        lines = []
        lines.append(pairs_sub[i])
        count = 0
        while count < 5:
            for j in range(len(pairs_sub)):
                if lines[-1][1] == pairs_sub[j][0]:
                    lines.append(pairs_sub[j])
                    count = 0
            count += 1
        multi_lines.append(lines)
    delete_lines = []
    for i in range(len(multi_lines)):
        for j in range(len(multi_lines)):
            if set(multi_lines[j]).issubset(multi_lines[i]) and i != j:
                delete_lines.append(j)
    delete_lines = list(set(delete_lines))    
    filtered_rows = [row for idx, row in enumerate(multi_lines) if idx not in delete_lines]
    final_order = flatten_tuples(filtered_rows,second_elements)
    
    #what happens if there are more tables? Joins with possible subqueries 
    for i in range(len(subq_from_form)):
        if subq_from_form[i][0] == 'Table':
            if 'AS' in subq_from_form[i][1]:
                table_subq = subq_from_form[i][1].split('AS')[0].strip()
            else:
                table_subq = subq_from_form[i][1]
    
    all_tuple =[]        
    for i in range(len(final_order)):
        for j in range(len(final_order[i])):
            if subq_select_form[final_order[i][j]][0] == 'Column':
                tuple_start = table_subq + '.' + subq_select_form[final_order[i][j]][1]
            if subq_select_form[final_order[i][j]][0] == 'Alias':
                tuple_end = sub_alias_dict[subqueries[e].alias] + '.' + subq_select_form[final_order[i][j]][1].rsplit('AS')[-1].strip() #atention here change when in loop
        all_tuple.append((tuple_start,tuple_end))
    
    bla.extend(all_tuple)





