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
                backward_count_to_from = 1
                is_tables_found = False
                while not is_tables_found:
                    if 'from' == parsed_sql.tokens[token_index - backward_count_to_from].value.lower():
                        break
                    backward_count_to_from += 1
                table_names = parsed_sql.tokens[token_index - (backward_count_to_from - 2)].value.replace(' ','').split(',')
                table_name = table_names[0]

                where_contains_parenthetic_stmt = False
                query = token.value
                for item in list(__parenthetic_contents(token.value)):
                    if 'select' in item[1].lower():
                        where_contains_parenthetic_stmt = True
                        x = sql_to_template(item[1])
                        query = query.replace(item[1], x)
                
                if not where_contains_parenthetic_stmt and table_name in list(parsed_sql.selectable_columns.keys()):
                    if len(parsed_sql.selectable_columns[table_name]) != 0: # only add PSP if there's selectable
                        column_name = parsed_sql.selectable_columns[table_name][-1]

                        token_value = token.value
                        if 'select' in token.value.lower() and str(token.ttype) != "Token.Keyword.DML":
                            token_value = __nested_sql_token_to_template(token_value)

                        if '{' not in token_value and '}' not in token_value: # check if it's recursive
                            if token_index == len(parsed_sql.tokens) - 1:
                                if ';' in token.value:
                                    processed_query_line = token_value.replace(";", " and {table_name}.{column_name} < {{}};".format(table_name=table_name, column_name=column_name))
                                else:
                                    processed_query_line = str(token_value) + " and {table_name}.{column_name} < {{}} ".format(table_name=table_name, column_name=column_name)
                            else:
                                processed_query_line = str(token_value) + " and {table_name}.{column_name} < {{}} ".format(table_name=table_name, column_name=column_name)

                            reconstructed_query.append(processed_query_line)
                        else:
                            reconstructed_query.append(token_value)
                else:
                    reconstructed_query.append(query)
                
            elif is_table:
                reconstructed_query.append(token.value)

                is_next_nonjoin_keyword_found, has_where = False, False
                while not is_next_nonjoin_keyword_found:
                    if token_index + 1 == len(parsed_sql.tokens):
                        break
                    token_next_keyword = parsed_sql.tokens[token_index + 1]
                    if str(token_next_keyword.ttype) == "Token.Keyword":
                        if any(keyword in token_next_keyword.value.lower() for keyword in ['join', 'on','and']):
                            pass
                        else:
                            is_next_nonjoin_keyword_found = True
                            break
                    else:
                        if 'where' in token_next_keyword.value.lower():
                            has_where = True
                            break
                    reconstructed_query.append(token_next_keyword.value)
                    token_index += 1   
                
                table_names = token.value.replace(' ','').split(',')
                table_name = table_names[0]

                if len(parsed_sql.selectable_columns[table_name]) != 0: # only add PSP if there's selectable
                    column_name = parsed_sql.selectable_columns[table_name][-1]
                    psp_statement = " where {table_name}.{column_name} < {{}} ".format(table_name=table_name, column_name=column_name)
                    try:
                        if has_where:
                            token_value = parsed_sql.tokens[token_index].value.lower()
                            token_value = token_value.replace('where', 'where {table_name}.{column_name} < {{}} and'.format(table_name=table_name, column_name=column_name))
                            reconstructed_query.append(token_value)
                        else:
                            has_where = True if 'where' in str(parsed_sql.tokens[token_index + 2].value.lower()) else False
                            if not has_where:
                                reconstructed_query.append(psp_statement)
                    except:
                        reconstructed_query.append(psp_statement)

            else:
                contains_parenthetic_stmt = False
                query = token.value
                for item in list(__parenthetic_contents(token.value)):
                    if 'select' in item[1].lower():
                        contains_parenthetic_stmt = True
                        x = sql_to_template(item[1])
                        query = query.replace(item[1], x)
                if contains_parenthetic_stmt:
                    reconstructed_query.append(query)
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
