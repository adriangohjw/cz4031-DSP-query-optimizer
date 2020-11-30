import database
import json
import os


def __store_summaries_in_file(summaries_json):
    if not os.path.exists('temp'):
        os.makedirs('temp')
    with open('temp/summaries.txt', 'w') as filehandle:
        json.dump(summaries_json, filehandle)


def __read_summaries_from_file():
    if not os.path.exists('temp') or not os.path.isfile('temp/summaries.txt'):
        print("No summaries file to read from")
        return None
    else:
        with open('temp/summaries.txt') as json_file:
            data = json.load(json_file)
            return data


def __get_all_table_names():
    query = """
        SELECT 
            table_name 
        FROM 
            information_schema.tables 
        WHERE 
            table_type = 'BASE TABLE' 
            AND table_schema = 'public';
    """

    connection = database.DBConnection()
    result = connection.execute(query)
    connection.close()

    tables = []
    for table_name in result:
        tables.append(table_name[0])

    return tables


def __get_record_count(table_name):
    query = """
        SELECT COUNT(*) 
        FROM {table_name};
    """.format(table_name=table_name)

    connection = database.DBConnection()
    result = connection.execute(query)
    connection.close()
    
    return result[0][0]


def __get_columns_ordinal_sorted(table_name):
    query = """
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = '{table_name}' 
        ORDER BY ORDINAL_POSITION
    """.format(table_name=table_name)

    connection = database.DBConnection()
    result = connection.execute(query)
    connection.close()

    columns = []
    for table_name in result:
        columns.append(table_name[0])

    return columns


def __get_column_uniqueness(table_name, col_name):
    query = """
        SELECT count(distinct({col_name})) * 100 
        FROM {table_name};
    """.format(col_name=col_name, table_name=table_name)

    connection = database.DBConnection()
    result = connection.execute(query)
    connection.close()

    return result[0][0]


def get_statiscal_summaries():
    summaries_json = __read_summaries_from_file()

    if summaries_json is None:
        print("Generating cardinality json...")

        summaries_json = []
        for table_name in __get_all_table_names():
            print("--- Generating for table: {}".format(table_name))
            record_count = __get_record_count(table_name)
            sorted_columns = __get_columns_ordinal_sorted(table_name)

            columns = []
            for col_name in sorted_columns:
                column_density = __get_column_uniqueness(table_name, col_name)
                if (record_count != 0):
                    columns.append({
                        "name": col_name, 
                        "density": column_density/record_count
                    })
                else:
                    columns.append({
                        "name": col_name, 
                        "density": 0
                    })

            columns = sorted(columns, key=lambda l:l["density"], reverse=True)
            summaries_json.append({
                "table_name": table_name, 
                "cardinality": record_count, 
                "sorted_columns": [col["name"] for col in columns]
            })

        summaries_json = sorted(summaries_json, key=lambda l:l['cardinality'], reverse=True)

        __store_summaries_in_file(summaries_json)

    return summaries_json
