import database

class ColumnSelector:
    def __init__(self, table_name):
        self.table_name = table_name
        self.attributes = None


    def get_attributes(self):
        self.__filter_datatype()
        self.__filter_permissible_string()
        self.__parse_column_pairs()
        return self.attributes


    def __filter_datatype(self):
        query = """
            SELECT 
                column_name, data_type
            FROM 
                information_schema.columns
            WHERE 
                table_name = '{table_name}'
                AND data_type IN (
                    'bigint', 'integer',
                    'double precision', 'real',
                    'character', 'character varying', 'text',
                    'date', 'timestamp without time zome', 'timestamp with time zone'
                )
        """.format(table_name=self.table_name)

        connection = database.DBConnection()
        result = connection.execute(query)
        connection.close()

        result_list = []
        for column_name, data_type in result:
            result_list.append({
                "column_name": column_name,
                "data_type": data_type,
            })
            
        self.attributes = result_list


    def __filter_permissible_string(self):
        result_list = []

        for item in self.attributes:
            if item['data_type'] in ['character', 'character varying', 'text']:
                query = """
                    SELECT count(*)
                    FROM {table_name}
                    WHERE {column_name} ~ '^.*[^A-Za-z0-9 .-].*$'
                """.format(
                    table_name=self.table_name, 
                    column_name=item['column_name']
                )
                
                connection = database.DBConnection()
                result = connection.execute(query)
                connection.close()

                count_non_alphanumeric_records = result[0][0]
                if count_non_alphanumeric_records != 0:
                    continue
                
            result_list.append(item)

        self.attributes = result_list


    def __parse_column_pairs(self):
        self.attributes = [column['column_name'] for column in self.attributes]


# get usable attributes for a given table using the following code
### ColumnSelector('users').get_attributes()
