import sql_parser
from sql_parser import SQLParser


def sql_to_template(raw_sql):
    parsed_sql = SQLParser(raw_sql)
    reconstructed_query = []

    token_index = 0
    while token_index < len(parsed_sql.tokens):
        token = parsed_sql.tokens[token_index]
        token_type = str(token.ttype)
            
        if token_type in ('None'):
            is_table = True

            for item in token.value.replace(' ','').split(','):
                if item not in (parsed_sql.tables):
                    is_table = False
                    break

            if 'where' in str(token.value).lower():
                table_names = parsed_sql.tokens[token_index - 2].value.replace(' ','').split(',')
                table_name = table_names[0]
                print(table_names)
                
                if len(parsed_sql.selectable_columns[table_name]) != 0: # only add PSP if there's selectable
                    print(parsed_sql.selectable_columns[table_name])
                    column_name = parsed_sql.selectable_columns[table_name][0] # TODO: update hard coded logic on which column to select as PSP

                    token_value = token.value
                    if 'select' in token.value.lower() and str(token.ttype) != "Token.Keyword.DML":
                        token_value = __nested_sql_token_to_template(token_value)

                    if token_index == len(parsed_sql.tokens) - 1:
                        processed_query_line = token_value.replace(";", " and {table_name}.{column_name} < {{}};".format(table_name=table_name, column_name=column_name))
                    else:
                        processed_query_line = str(token_value) + " and {table_name}.{column_name} < {{}} ".format(table_name=table_name, column_name=column_name)
                    reconstructed_query.append(processed_query_line)
                
            elif is_table:
                reconstructed_query.append(token.value)

                table_names = token.value.replace(' ','').split(',')
                table_name = table_names[0]

                if len(parsed_sql.selectable_columns[table_name]) != 0: # only add PSP if there's selectable
                    column_name = parsed_sql.selectable_columns[table_name][0] # TODO: update hard coded logic on which column to select as PSP
                    psp_statement = " where {table_name}.{column_name} < {{}} ".format(table_name=table_name, column_name=column_name)
                    try:
                        has_where = True if 'where' in str(parsed_sql.tokens[token_index + 2].value.lower()) else False
                        if not has_where:
                            reconstructed_query.append(psp_statement)
                    except:
                        reconstructed_query.append(psp_statement)

            else:
                reconstructed_query.append(token.value) 

        else:
            reconstructed_query.append(token.value) 

        token_index += 1

    reconstructed_query = ''.join(map(str, reconstructed_query))
    return reconstructed_query


def __nested_sql_token_to_template(nested_sql):
    reconstructed_token_value = nested_sql
    for content in list(__parenthetic_contents(nested_sql)):
        if 'select' in content[1].lower():
            temp_parsed_sql = SQLParser(content[1])
            reconstructed_token_value = reconstructed_token_value.replace(content[1], sql_to_template(content[1]))
    return reconstructed_token_value


def __parenthetic_contents(string):
    """Generate parenthesized contents in string as pairs (level, contents)."""
    stack = []
    for i, c in enumerate(string):
        if c == '(':
            stack.append(i)
        elif c == ')' and stack:
            start = stack.pop()
            yield (len(stack), string[start + 1: i])


# get usable attributes for a given table using the following code
### raw_sql = "select c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, sum(l_quantity) from customer, orders, lineitem where o_orderkey in ( select l_orderkey from lineitem group by l_orderkey having sum(l_quantity) > 314 ) and c_custkey = o_custkey and o_orderkey = l_orderkey group by c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice order by o_totalprice desc, o_orderdate limit 1;"
### picasso_query_template = sql_to_template(raw_sql)
### print(sql_parser.format(picasso_query_template))
