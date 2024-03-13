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



#This file is for creating tuples out of the incoming tsql scripts for further use in visualising lineages. The variable "tuples" should contain all the source and target pairs.
#Needs substitution of "hardcoded_dict" with database table extractor.


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

froms = repr(ast)


table_alias = list(ast.find_all(exp.Table))


space_table = []
for element in table_alias:
    if " " in element.name:
        space_table.append((element.name.replace(" ",""),element.name))

space_table = list(set(space_table))




subqueries = list(ast.find_all(exp.Subquery))

    
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


alias_table = alias_table + subquery_aliases

pseudo_tables = []
for element in subqueries:
    variables = list(set([x.name for x in element.find_all(exp.Column)]))
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
        

_subquery_alias=[]
_subquery_columns=[]
for subs in subqueries:
    columns2 = subs.this.args["expressions"]
    for element in columns2:
        temp = element.this
        while isinstance(temp, str) ==False:
            if isinstance(temp, exp.Star):
                _subquery_columns.append("*")
                break
            temp = temp.this

        if isinstance(temp, str):
            _subquery_columns.append(temp)
   
    for element in columns2:
        if isinstance(element, exp.Alias):
            _subquery_alias.append(element.alias)
        else:
            _subquery_alias.append("NoAlias")
        

subquery_matched = list(zip(_subquery_columns, _subquery_alias))
        

def transformer_column(node):
    for element in subquery_matched:
        if isinstance(node, exp.Column) and node.name == element[1]:
            return parse_one(node.table + "." + element[0])
    return node


def transformer_table(node):
    for element in alias_table:
        if isinstance(node, exp.Column) and node.table == element[1]:
            return parse_one(element[0] + "." + node.name)
    return node

def transformer_original(node):
    for element in pseudo_tables:
        for i,j in element.items():
            for l in j:
                if isinstance(node, exp.Column) and node.table =="" and node.name ==l:
                    return parse_one(i + "." + l)
    return node


transformed_tree = ast.transform(transformer_column).transform(transformer_table).transform(transformer_original)
transformed_trial =transformed_tree.sql(dialect = 'tsql')

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
    

target =[]
for element in insert_expressions:
    temp = []
    for i in element[:-1]:
        temp.append(element[-1]+"."+i)
    target.append(temp)

target_final =[]
for element in target:
    for l in element:
        target_final.append(l)
    
source = []
new_columns =[]
select_statement = transformed_tree.selects
for element in select_statement:
    temp = list(element.find_all(exp.Column))
    new_columns.append(temp)
    
    
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

for element in source:
    if element ==[]:
        source.remove(element)


tuples_list= []
for k, element in enumerate(source):
    temp =[]
    position = k
    for i in element:
        temp.append((i,target_final[position]))
    tuples_list.append(temp)



tuples = []
for element in tuples_list:
    for i in element:
        tuples.append(i)
        
    
    
    
    
    
    
    