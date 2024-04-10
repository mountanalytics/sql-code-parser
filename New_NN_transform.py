import sqlglot
from sqlglot import parse_one, exp
from sqlglot.dialects.tsql import TSQL
from sqlglot.dialects.column import COLUMN
from sqlglot.dialects.hana import HANA
from sqlglot.dialects.ma import MA
import pandas as pd
 


# For this file to work you need to have the newest SQL Glot package installed from our own copy
# of the repository: pip install git+https://github.com/KaiserM11/sqlglot_ma

query = """
SELECT
    YEAR("NetDueDate"),
    CONCAT(CONCAT("CompanyCode",' - '), "CompanyCodeText"),
    TO_INT(DAYS_BETWEEN("NetDueDate", "$$IP_AgingDate$$")),
    TO_DATE("NetDueDate"),
    CONCAT(CONCAT("Country",' - '), "Countrytext"),
    CONCAT(CONCAT("Vendor",' - '), "VendorText"),
    TO_VARCHAR ("DocDate", 'YYYY-MM')
"""


df_tf = pd.read_excel("C:/Users/MátéKaiser/OneDrive - Mount Analytics/Desktop/functions.xlsx")
lookup_list = list(df_tf["Parser Keyword"])

def SQL_transf(query, flookup, inputl = "hana", outputl = "ma"):
    
    """ Function is designed to extract all the functions from a given sql script and convert them into the desired
    dialect. Returns a list of tuples where the first element is the recognised function and the second one
    is the redesigned function. The first argument is the raw query that needs translation, and it always
    has to be accompanied by a starting SELECT statement. Second argument is the lookup list that should contain all
    the function keywords that need to be looked for within the raw query. inputl and outputl are the input language and
    the output language respectively. 
    """
    ast = parse_one(query, dialect = inputl)
    
    
    org_columns = list(ast.find_all(exp.Column))
    cleaned_columns = []
    
    for element in org_columns:
        if "$" in element.name:
            cleaned_columns.append((element.name,element.name.replace("$", "")))

    
    def transformer_column(node):
        for element in cleaned_columns:
            if isinstance(node, exp.Column) and node.name == element[0]:
                return parse_one('"'+element[1]+'"')
        return node

    cleaned_tree = ast.transform(transformer_column)

    
    general_syntax= []
    transformations = []
    scripts = []
    for i in flookup:    
        o = list(cleaned_tree.find_all(getattr(exp, i)))
        for element in o:
            general_syntax.append(element.sql(dialect = outputl))
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
    


    return mother_expressions, scripts



pop, script = SQL_transf(query, lookup_list)


