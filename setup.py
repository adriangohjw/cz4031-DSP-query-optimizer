from column_selector import ColumnSelector
from statistical_summaries import get_statiscal_summaries, __get_all_table_names

tables = __get_all_table_names()
for table_name in tables:
    cs = ColumnSelector(table_name)
    cs.get_attributes()

get_statiscal_summaries()
