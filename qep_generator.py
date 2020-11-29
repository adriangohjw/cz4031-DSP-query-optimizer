import psycopg2
from psycopg2 import sql
import pandas as pd
from tqdm import tqdm
import database
from sql_to_template_converter import sql_to_template
import sql_parser
import re

class generateQEPs:
    
    def __init__(self, sql_query):
        self.connection = database.DBConnection()
        self.sql_query = sql_query
        self.template_query = sql_parser.format(sql_to_template(self.sql_query))
        self.predicates = self.predicate_cols_names()
        self.partition1, self.partition2 = 100, 100
        
    def predicate_cols_names(self):
        li = re.findall('[a-z]+...[a-z]+ < {}', self.template_query)
        li2 = []
        for i in range(len(li)):
            a = re.findall('[a-z]+\.', li[i])
            a = a[0][:len(a[0])-1]
            b = re.findall('[_a-z]+ <', li[i])
            b = b[0][:len(b[0])-2]
            li2.append((b,a))
        return li2
    
    def execute_query(self, query_text):
        query_text = sql.SQL(query_text)
        result = self.connection.execute(query_text)
        return result
    
    def get_dbms_qep(self):
        result = self.execute_query('EXPLAIN (ANALYZE, COSTS, VERBOSE, BUFFERS, FORMAT JSON ) ' + self.sql_query)
        return result
    
    def get_equal_height_histogram_query(self, column, table, num_partition):
        return 'SELECT ntile, min({}), max({}) ' \
                'FROM ( SELECT {}, ntile({}) over (order by {}) ' \
                'FROM {}) x ' \
                'GROUP BY ntile ' \
                'ORDER BY ntile'.format(column, column, column, num_partition, column, table)
    
    def get_partitions(self):
        part1, part2 = self.predicates

        part1_divisions = []
        results = self.execute_query(self.get_equal_height_histogram_query(part1[0], part1[1], self.partition1))
        for _, _, max_partition in results:
            part1_divisions.append(max_partition)

        part2_divisions = []
        results = self.execute_query(self.get_equal_height_histogram_query(part2[0], part2[1], self.partition2))
        for _, _, max_partition in results:
            part2_divisions.append(max_partition)

        return part1_divisions, part2_divisions
    
    def get_alternate_qeps(self):
        p1, p2 = self.get_partitions()

        results = dict()
        i = 0
        j = 0
        rows_list = []
        for s in tqdm(p1):
            i += 1
            j = 0
            for l in p2:
                j += 1
                part1, part2 = self.predicate_cols_names()
                replace_part1_prefix = part1[0]+" < "
                replace_part2_prefix = part2[0]+" < "
                query = 'EXPLAIN (FORMAT JSON, COSTS FALSE)' + self.template_query
                result = ((self.execute_query(query.format(s,l))))
                plan = str(result[0][0][0]['Plan']).replace("'","").replace(replace_part1_prefix+str(s),"").replace(replace_part2_prefix+str(l),"").replace(replace_part1_prefix+str(int(s)),"").replace(replace_part2_prefix+str(int(l)),"")
                results[plan] = [i, j]

        alt_qeps = []
        for i in tqdm(results.values()):
            query = 'EXPLAIN (ANALYZE, COSTS, VERBOSE, BUFFERS, FORMAT JSON ) ' + self.template_query
            result = ((self.execute_query(query.format(i[0],i[1]))))
            alt_qeps.append(result)
            
        return alt_qeps

# f = open('18.sql', 'r')
# content = f.read()
# a = generateQEPs(content)
# alt = a.get_alternate_qeps()
# best = a.get_dbms_qep()