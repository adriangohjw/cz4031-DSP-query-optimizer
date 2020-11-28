import itertools
import sqlparse

from sqlparse.sql import IdentifierList, Identifier
from sqlparse.tokens import Keyword, DML

from column_selector import ColumnSelector


class SQLParser:
    def __init__(self, query):
        self.query = query
        self.tokens = sqlparse.parse(self.query)[0].tokens
        self.tables = self.__extract_tables()
        self.selectable_columns = self.__get_selectable_columns()
        self.splitted_query = self.__splitted()


    def __splitted(self):
        statements = sqlparse.split(self.query)
        statement = statements[0]
        sql_parsed = sqlparse.format(statement, reindent=True, keyword_case='upper')
        sql_parsed_split = sql_parsed.splitlines()
        return sql_parsed_split

    
    def __get_selectable_columns(self):
        columns_to_check = { 'unknown': [] }
        for table in self.tables:
            columns_to_check[table] = []
        
        # remove columns if they are in 'where' statement
        for token in self.tokens:
            if 'where' in token.value.lower():
                for word in token.value.split(' '):
                    if '.' in word:
                        word_splitted = word.split('.')
                        table_name = word_splitted[0]
                        column_name = word_splitted[1]
                        if table_name in columns_to_check:
                            columns_to_check[table_name].append(column_name)
                        else:
                            columns_to_check['unknown'].append(column_name)    
                    else:
                        columns_to_check['unknown'].append(word)

        selectable_columns = {}
        for table in self.tables:
            selectable_columns_for_table = ColumnSelector(table).get_attributes()
            selectable_columns_for_table = [x for x in selectable_columns_for_table if x not in columns_to_check[table]]
            selectable_columns_for_table = [x for x in selectable_columns_for_table if x not in columns_to_check['unknown']]
            selectable_columns[table] = selectable_columns_for_table
        return selectable_columns


    def __extract_tables(self):
        extracted_tables = []
        statements = list(sqlparse.parse(self.query))
        for statement in statements:
            if statement.get_type() != 'UNKNOWN':
                stream = self.__extract_from_part(statement)
                extracted_tables.append(set(list(self.__extract_table_identifiers(stream))))
        return list(itertools.chain(*extracted_tables))


    def __is_subselect(self, parsed):
        if not parsed.is_group:
            return False
        for item in parsed.tokens:
            if item.ttype is DML and item.value.upper() == 'SELECT':
                return True
        return False


    def __extract_from_part(self, parsed):
        from_seen = False
        for item in parsed.tokens:
            if item.is_group:
                for x in self.__extract_from_part(item):
                    yield x
            if from_seen:
                if self.__is_subselect(item):
                    for x in self.__extract_from_part(item):
                        yield x
                elif item.ttype is Keyword and item.value.upper() in ['ORDER', 'GROUP', 'BY', 'HAVING', 'GROUP BY']:
                    from_seen = False
                    StopIteration
                else:
                    yield item
            if item.ttype is Keyword and item.value.upper() == 'FROM':
                from_seen = True


    def __extract_table_identifiers(self, token_stream):
        for item in token_stream:
            if isinstance(item, IdentifierList):
                for identifier in item.get_identifiers():
                    value = identifier.value.replace('"', '').lower()                
                    yield value
            elif isinstance(item, Identifier):
                value = item.value.replace('"', '').lower()
                yield value
