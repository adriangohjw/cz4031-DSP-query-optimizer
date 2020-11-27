import sqlparse

def parseSplitSQL(raw_sql_query):

    statements = sqlparse.split(raw_sql_query)
    statement = statements[0]

    sql_parsed = sqlparse.format(statement, reindent=True, keyword_case='upper')
    sql_parsed_split = sql_parsed.splitlines()

    return sql_parsed_split
    