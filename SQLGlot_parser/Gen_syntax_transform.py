import sqlglot
from sqlglot import parse_one, exp
from sqlglot.dialects.tsql import TSQL
import pandas as pd
 
""" Python file for transforming the incoming tsql scripts into our own dialect specified in the transformation library,
    in order for this to work you have to install our custom made sqlglot package. Type into the terminal:
    pip install git+https://github.com/KaiserM11/sqlglot_ma
	
"""
query = """

SELECT 
	ProductID, 
	ProductName,
    round((cast(a.quantity as numeric (10,2))/cast(d.product_quantity_year as numeric (10,2)))*100,1) as perc_of_product_quantity_year,
	case 
		when QuantityPerUnit like '%bottles' THEN 'BEVERAGE'
		ELSE NULL END AS beverage,  
	Sum(SupplierID),
    Round(SupplierID, 1),
	Count(CategoryID), 
	QuantityPerUnit, 
	Avg(UnitPrice),
    Min(supplierID),
    Max(supplierID),
    Cast(quantity as numeric (10,2)),
    DateAdd(month, 1, "ShippedDate"),
    DateDiff(year, '2017/08/25', '2011/08/25'),
    DatePart(year, '2017/08/25'),
    Day('2017/08/25'),
    Month('2017/08/25'),
    Year('2017/08/25'),
    Rtrim(quantity),
    LTrim(quantity),
    Current_Timestamp,
    Abs(-3),
    Power(12, 2),
    IIF(500<1000, 'YES', 'NO'),
    Substring(SupplierID),
    Coalesce('3', '2', Null),
    NULLIF(25, 25),
    REPLACE(      'SQL Tutorial',  
            'T',           'M'),
    1 >2,
    54*1,
    54-12,
    43+23,
    5<1,
    4<=41,
    34>531,
    43!=43,
    13<>56,
    quantity IN '12',
    qunatity Between '13' And '11',
    '12' or '14',
    quantity Like '%12',
    Concat('he', 'she', 'it', 'they', 'them'),
    Isnull('41', '18'),
    12 !> 15,
    32 !< 19,
	case 
		when UnitsInStock < UnitsOnOrder THEN CAST((UnitsInStock-UnitsOnOrder) as VARCHAR)
		WHEN UnitsOnOrder > 0 and UnitsInStock = UnitsOnOrder THEN 'No stock left'
		WHEN UnitsOnOrder > 0 and (UnitsInStock - UnitsOnOrder) < ReorderLevel THEN 'time to reorder'
		ELSE NULL END AS StockShortage, 
	UnitsInStock, 
	UnitsOnOrder, 
	ReorderLevel, 
	Discontinued
FROM 	
	MA_NorthWindDB.dbo.Products
WHERE year = 2013 AND year_rank NOT Between 1 and 2
"""



#date_delta_sql is the key to datetime object renaming and restructuring!!!!


df_tf = pd.read_excel("C:/Users/MátéKaiser/OneDrive - Mount Analytics/Desktop/functions.xlsx")
lookup_list = list(df_tf["Parser Keyword"])

ast = parse_one(query, read = 'tsql')
whole_script = repr(ast)


transfer_back  = ast.sql(dialect = 'tsql')



tests = list(ast.find_all(exp.GT))
trim_syntax = []
for element in tests:
    trim_syntax.append(repr(element))


anonymous = list(ast.find_all(exp.Anonymous))
a_syntax = []
for element in anonymous:
    a_syntax.append(repr(element))

general_syntax= []
transformations = []
scripts = []
for i in lookup_list:    
    o = list(ast.find_all(getattr(exp, i)))
    for element in o:
        general_syntax.append(element.sql(dialect = "tsql"))
        scripts.append(repr(element))
        if len(o)>0:
            transformations.append(i)
    
matched = list(zip(transformations, general_syntax))


mother_expressions = []
for expression1 in matched:
    is_mother = True
    for expression2 in matched:
        if expression1[1] != expression2[1] and expression1[1] in expression2[1]:
            is_mother = False
            break
        
    if is_mother: 
        mother_expressions.append(expression1)

    
ifs = []
cases = []

for i, element in list(enumerate(mother_expressions)):
    if element[0] == "If":
        ifs.append((i,element[1].replace(" ","").replace("(", "").replace(")", "").strip("IFTHENELSE")))
    elif element[0] == "Case":
        cases.append((i, element[1].replace(" ","").replace("(", "").replace(")", "").strip("IFTHENELSE")))

singular_if = []
for element in ifs:
    main = False
    for i in cases:
       if element[1] in i[1]:
            main = True
    if main:
        singular_if.append(element)

index_remove =[]
for element in singular_if:
    index_remove.append(element[0])

list_remove=[]
for element in index_remove:
    for l in mother_expressions:
        if element == mother_expressions.index(l):
            list_remove.append(l)
        
for element in list_remove:
    mother_expressions.remove(element)

#ast = parse_one(query, read="tsql")
#b = ast.sql(dialect = 'tsql')
#c= repr(ast)
#table_alias = list(ast.find_all(exp.Mul))
#list(ast.find_all(exp.Alias))




