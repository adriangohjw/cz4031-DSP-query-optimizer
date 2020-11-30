import database
import os.path

class ColumnSelector:
    def __init__(self, table_name):
        self.table_name = table_name
        self.attributes = None


    def get_attributes(self):
        if os.path.isfile('temp/{}.txt'.format(self.table_name)):
            self.attributes = self.__read_attributes_from_file()
        else:
            self.__filter_datatype()
            self.__filter_permissible_string()
            self.__parse_column_pairs()
            self.__filter_foreign_keys()
            self.__store_attributes_in_file()
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


    def __filter_foreign_keys(self):
        query = """
            SELECT
                kcu.column_name
            FROM 
                information_schema.table_constraints AS tc 
                JOIN information_schema.key_column_usage AS kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
                JOIN information_schema.constraint_column_usage AS ccu
                ON ccu.constraint_name = tc.constraint_name
                AND ccu.table_schema = tc.table_schema
            WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_name='{table_name}';
        """.format(table_name=self.table_name)
        
        connection = database.DBConnection()
        result = connection.execute(query)
        connection.close()

        foreign_keys = []
        for column_name in result:
            foreign_keys.append(column_name[0])
            
        self.attributes = [item for item in self.attributes if item not in foreign_keys]


    def __parse_column_pairs(self):
        self.attributes = [column['column_name'] for column in self.attributes]


    def __read_attributes_from_file(self):
        attributes = []
        with open('temp/{}.txt'.format(self.table_name), 'r') as filehandle:
            for line in filehandle:
                currentPlace = line[:-1] # remove linebreak which is the last character of the string
                attributes.append(currentPlace)
        return attributes


    def __store_attributes_in_file(self):
        if not os.path.exists('temp'):
            os.makedirs('temp')
        with open('temp/{}.txt'.format(self.table_name), 'w') as filehandle:
            for item in self.attributes:
                filehandle.write('{}\n'.format(item))


# get usable attributes for a given table using the following code
### ColumnSelector('users').get_attributes()
