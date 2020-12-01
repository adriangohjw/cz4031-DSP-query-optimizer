import os
import sql_parser
from sql_to_template_converter import sql_to_template


def __read_scripts_from_file(filename):
    fd = open(filename, 'r')
    sqlFile = fd.read()
    fd.close()

    sqlCommands = sqlFile.split(';')
    query = sqlCommands[0]
    return query


def test_suite(specific_file=None):
    for filename in os.listdir('test_queries'):
        if filename.endswith(".sql"): 
            if specific_file is None:
                pass
            else:
                if not filename == specific_file:
                    continue

            fd = open('test_queries/{}'.format(filename), 'r')
            sqlFile = fd.read()
            fd.close()

            print('testing {}................................................................'.format(fd.name))
            query = __read_scripts_from_file('test_queries/{}'.format(filename))
            picasso_query_template = sql_to_template(query)
            print(sql_parser.format(picasso_query_template))
            print()


test_suite()
