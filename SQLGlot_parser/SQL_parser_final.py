from sqlglot import parse_one, exp
from sqlglot.dialects.tsql import TSQL
import pypyodbc as odbc
odbc.lowercase = False
import configparser
import copy
from collections import defaultdict
from collections import OrderedDict 
import pandas as pd
import pypyodbc as odbc
import configparser
odbc.lowercase = False


"""
Trial for extracting eveything from the subqueries and stockpiling them in the SELECT statement of the main query, first replacing
the column names and then the table aliases to create an easier lineage extract possibility.
"""

#This line to be repalced by the code to extract the tables and columns from a specific database (Erwin's database extractor tool)
hardcode_dict = [{"MA_NorthWindDB.Order Details": ["OrderID", "ProductID", "UnitPrice", "Quantity", "Discount"]}, {"dbo.Order Details": ["Product", "Sale", "Prices"]} ]


query = """
INSERT INTO MA_NorthWindDB.dbo.Order_Details_1998_Extract(star1, star2, star3, star4, star5,orderid,star21, star22, star23 NrOfProducts, avg_order_unitprice, max_order_discount, min_order_discount, total_quantity, product_quantity_year, 	perc_of_product_quantity_year)

SELECT
    a.*,
    orderid,
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
				max(avg(unitprice)) as avg_order_unitprice,
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


#Snippet for finding all table names which have an empty space in them and storing them without the " " for later use.
table_names = list(ast.find_all(exp.Table))
space_table = []
for element in table_names:
    if " " in element.name:
        space_table.append((element.name.replace(" ",""),element.name))

space_table = list(set(space_table)) # a list of tuples with table names paired (space removed original - original ) Eg. (OrderDetails, Order Details)



# Code snippet for extracting the table names and their aliases, used to reconstruct a tuple with structure (database+schema+name, alias )
table_alias = list(ast.expression.find_all(exp.Table))
alias_table = []
for element in table_alias:
    table_alias =  element.alias
    table_name = element.name
    table_db = element.db
    table_catalog = element.catalog
        
    if " " in table_name:
        table_name = table_name.replace(" ", "")

    if table_catalog == "":    
        alias_table.append((table_db+"."+table_name, table_alias))
    elif table_db == "":
        alias_table.append((table_catalog+"."+table_name, table_alias))
    else:
        alias_table.append((table_catalog+"."+ table_db+"."+table_name, table_alias))

#The object alias_table contains all the (database+table, alias) tuple combinations for later use and replacement.

#Once again extracting the table names and aliases but this time for the tables mentioned in the subqueries of the sql script.
#The only drawback is that this only works when the subquery is only one level nested and only one table is mentioned in the From statement of the subquery.
#It pairs the table name with the alias of the whole subquery.
subqueries = list(ast.find_all(exp.Subquery))   
subquery_aliases = [] 
if len(subqueries) > 0:
    for element in subqueries:
            alias = element.alias
            source = element.this.args["from"]
            table= source.this.name
            catalog =  source.this.catalog
            db = source.this.db
            
            
            if " " in table:
                table = table.replace(" ", "")

            if catalog == "":    
                subquery_aliases.append((db+"."+table, alias))
            elif db == "":
                subquery_aliases.append((catalog+"."+table, alias))
            else:
                subquery_aliases.append((catalog+"."+ db+"."+table, alias))
                
#The object subquery_aliases is a list of tuples with the alias strcuture (database name + table name , alias of the subquery)

alias_table = alias_table + subquery_aliases #Adding the table+alias tuples from the subqueries to the general alias_table object.


#Below code snippet iterates over the subqueries, collects the table name from the FROM statement and also paires it with the mentioned column objects.
#pseudo_tables object is a list of dictionaries where the key is the table name from the FROM statement and the values are all the columns
#mentioned in the subquery. Basicly creating artificial tables from the subqueries and storing them in a dictionary format. 
pseudo_tables = []
for element in subqueries:
    
    expressions = list(element.this.expressions)
    column_objects = [list(x.find_all(exp.Column)) for x in expressions]
    variables = []
    for l in column_objects:
        for o in l:
            variables.append(o.name)
            
    variables = list(set(variables))
    alias = element.alias
    source = element.this.args["from"]
    table= source.this.name
    catalog =  source.this.catalog
    db = source.this.db
    complete_table = ""
    if " " in table:
        table = table.replace(" ", "")
    if catalog =="" and db =="":
        complete_table = table
    elif catalog == "":    
        complete_table = db + "." + table
    elif db == "":
        complete_table = catalog + "." + table
    else:
        complete_table = catalog + "." +  db +"." + table
    temp  = {}
    temp[complete_table] = variables
    pseudo_tables.append(temp)



#The below snippet iterates over the subqueries and extracts all the column names with their respective aliases. It does this by going over
#a tree branch and looking for values in the column objects that are strings. It can also identify if the column call is a star. If no alias was
#used the code returns "NoAlias" in place of the alias.
column_subquery_alias=[]
name_subquery_columns=[]
for subs in subqueries:
    columns2 = subs.this.args["expressions"]
    for element in columns2:
        temp = element.this
        while isinstance(temp, str) ==False:
            if isinstance(temp, exp.Star):
                name_subquery_columns.append("*")
                break
            temp = temp.this

        if isinstance(temp, str):
            name_subquery_columns.append(temp)
   
    for element in columns2:
        if isinstance(element, exp.Alias):
            column_subquery_alias.append(element.alias)
        else:
            column_subquery_alias.append("NoAlias")
        
subquery_matched = list(zip(name_subquery_columns, column_subquery_alias))
#The resulting object subquery_matched, is a list of tuples with the following structure (subquery column name, alias used in subquery)




#SQLGlot's special functions are transformer functions which iterate over each node and make changes on the way. These functions are designed
#for this specific code.

def transformer_column(node):
    """This first function loops over the main SELECT node and replaces column names that appear in the subqueries as aliases"""
    for element in subquery_matched:
        if isinstance(node, exp.Column) and node.name == element[1]:
            return parse_one(node.table + "." + element[0])
    return node


def transformer_table(node):
    """This function goes over the main SELECT node and replaces table aliases with either the whole table name or the subquery alias"""
    for element in alias_table:
        if isinstance(node, exp.Column) and node.table == element[1]:
            return parse_one(element[0] + "." + node.name)
    return node

def transformer_subquery(node):
    """This function loops over the SELECT statement arguments and replaces any missing tables or aliases with the paired table+column pair"""
    for element in pseudo_tables:
        for i,j in element.items():
            for l in j:
                if isinstance(node, exp.Column) and node.table =="" and node.name ==l:
                    return parse_one(i + "." + l)
    return node

#All three previously created functions are called together to create "transformed _trial" object that is a new SQL script with the 
#columns and tables replaced with the source information.
transformed_tree = ast.transform(transformer_column).transform(transformer_table).transform(transformer_subquery)
transformed_trial =transformed_tree.sql(dialect = 'tsql')


#Below code is to extract information and create table+column pairs of the targets that are found in the INSERT INTO statement.
insert_obj = list(ast.find_all(exp.Insert))

insert_expressions = []
for element in insert_obj:
    table = element.this.this
    table_name = table.name
    catalog =  table.catalog
    db = table.db
    if " " in table_name:
        table_name = table_name.replace(" ", "")
    if catalog =="" and db =="":
        insert_into = table_name
    if catalog == "":    
        insert_into = db + "." + table_name
    elif db == "":
        insert_into = catalog + "." + table_name
    else:
        insert_into = catalog + "." +  db +"." + table_name
    

    expressions = element.this.expressions
    column_names =[]
    for i in expressions:
        column_names.append(i.name)
    column_names.append(insert_into)
    insert_expressions.append(column_names)
#The object insert_expressions contains the columns mentioned in the insert expressions, and the last element of the list is the target table.


#The below snippets create the target table+column combinations from the insert_expressions. Target_final is a list of target expressions.
target_temp =[]
for element in insert_expressions:
    temp = []
    for i in element[:-1]:
        temp.append(element[-1]+"."+i)
    target_temp.append(temp)

target_final =[]
for element in target_temp:
    for l in element:
        target_final.append(l)




#The below snippets create the source combinations of table and columns. First it iterates over the new transformed SQL statement and 
#stores the column objects in new_columns.
source = []
new_columns =[]
select_statement = transformed_tree.selects
for element in select_statement:
    temp = list(element.find_all(exp.Column))
    new_columns.append(temp)
    
#Iterating over the new_columns objects and creating the source pairing of table+column name from the new tree.
for element in new_columns:
    temp = []
    for i in element:
        table = i.table
        catalog = i.catalog
        db = i.db
        column = i.name
        for w in space_table:
            if table == w[0]:
                table = w[1]
                
        if catalog =="" and db =="":
            final = table +"." +column
            matching = table
        if catalog == "":    
            final = db + "." + table + "."+column
            matching = db + "." + table 
        elif db == "":
            final = catalog + "." + table +"." +column
            matching = catalog + "." + table
        else:
            final = catalog + "." +  db +"." + table +"." +column
            matching = catalog + "." +  db +"." + table 
            
        if column =="*":
            for n in hardcode_dict:
                for y,u in n.items():
                    if y == matching:
                        for o in u:
                            source.append([matching+"."+o])
        else:
            temp.append(final)
    source.append(temp)
#Source object is a list of lists containing table+column combinations for each target mentioned in the insert statement. It also pairs 
# compound targets into one list, where the target column is made up of multiple source columns through transformations. 


#Removing empty lists from the source lists
for element in source:
    if element ==[]:
        source.remove(element)

#Unpacking and pairing up the source and the target information into tuples into the structure (source, target) 
tuples_list= []
for k, element in enumerate(source):
    temp =[]
    position = k
    for i in element:
        temp.append((i,target_final[position])) #Pairing source and target based on indexing of the source and target lists.
    tuples_list.append(temp)


tuples = []
for element in tuples_list:
    for i in element:
        tuples.append(i)

#The final outcome, the tuples list object is a list of paired source target tuples
#This also unpacks the compound source targets into separate lines.


#Extracting transformations from the new tree structure (not finished yet)
df_tf = pd.read_excel("C:/Users/MátéKaiser/OneDrive - Mount Analytics/Desktop/functions-new.xlsx")
lookup_list = list(df_tf["Parser Keyword"])
    
general_syntax= []
transformations = []
scripts = []
for i in lookup_list:    
    o = list(transformed_tree.find_all(getattr(exp, i)))
    for element in o:
        general_syntax.append(element.sql(dialect = "tsql"))
        scripts.append(repr(element))
        if len(o)>0:
            transformations.append(i)
        
matched = list(zip(transformations, general_syntax))
    
    
    
    
    