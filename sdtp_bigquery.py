from sdtp import InvalidDataException, jsonifiable_column, jsonifiable_rows, SDTPTable
from sdtp import SDTP_BOOLEAN, SDTP_DATETIME, SDTP_STRING, SDTP_NUMBER
from google.cloud import bigquery
import pandas as pd
class BigQueryTable:
    '''
    This is an interface class between SDTPBigQueryTable and BigQuery.  We use 
    this for a couple of reasons:
    1. The BigQuery interface can be tested separately from the SDTP interface; this is
    particularly important because they have different dependencies and authentication structures
    2. We can use the basic functionality in other contexts, e.g. a composite of
    a couple of BigQueryTables
    '''
    def __init__(self, project, dataset, table, client):
        self.bigquery_table_ID = f'{project}.{dataset}.{table}'
        self.project = project
        self.dataset = dataset
        self.table = table
        self.client = client
        self.schema = self.get_schema()

    def get_schema(self):
        bq_table = self.client.get_table(self.bigquery_table_ID)
        schema = [f for f in bq_table.schema]
        for field in schema:
            try:
                field["type"] = BQ_TYPE_MAPS[field["field_type"]]
            except KeyError:
                field["type"] = SDTP_STRING
        return schema
        
    
    def _execute_query(self, query):
        # Execute the SQL Query in the BigQuery Table
        query_job = self.client.query(query)
        rows = query_job.result()
        return rows.to_dataframe()
    
    
    def all_values(self, column_name: str):
        query = f'SELECT DISTINCT {column_name} from {self.bigquery_table_ID}'
        query_result = self._execute_query(query)
        result = query_result[column_name].tolist()
        result.sort()
        return  result
    
    def range_spec(self, column_name: str):
        # yes, BigQuery SQL supports a max and a min function.  BUT BigQuery charges by bytes processed,
        # a max and a min process all rows in a table, and so two queries are double the cost of one all_values  
        values = self.all_values(column_name)
        return {'max_val': values[-1], 'min_val': values[0] }

    def get_rows(self):
        
        query = 'select *'  f' from {self.bigquery_table_ID}'
        query_result = self._execute_query(query)
        return query_result
    
BQ_TYPE_MAPS = {
    "STRING": SDTP_STRING,
    "INTEGER": SDTP_NUMBER,
    "FLOAT":  SDTP_NUMBER,
    "BOOLEAN": SDTP_BOOLEAN,
    "TIMESTAMP": SDTP_DATETIME
}

def make_bigquery_table(project, dataset, table, client):
    bq_table = BigQueryTable(project, dataset, table, client)
    return SDTPBigQueryTable(bq_table)
    
    
            
    

class SDTPBigQueryTable(SDTPTable):
    '''
    Query a BigQuery Table.  The schema is as for every other table, and
    the three methods are SQL queries over the table
    '''
    def __init__(self,  bigquery_table):
        super(SDTPBigQueryTable, self).__init__(bigquery_table.schema)
        self.bigquery_table = bigquery_table
  
    def _get_column_index(self, column_name):
        try:
            return self.column_names().index(column_name)
        except ValueError as original_error:
            raise InvalidDataException(f'{column_name} is not a column of this table') from original_error
    
    def all_values(self, column_name: str, jsonify = False):
        index = self._get_column_index(column_name)
        sdtp_type = self.schema[index]["type"]
        result = self.bigquery_table.all_values(column_name)
        return jsonifiable_column(result, sdtp_type) if jsonify else result
    
    def range_spec(self, column_name: str, jsonify = False):
        index = self._get_column_index(column_name)
        sdtp_type = self.schema[index]["type"]
        return self.bigquery_table.range_spec(column_name) # need to jsonify!
        result = {}
        for field in ['max', 'min']:
            query = f'SELECT {field}({column_name}) from {self.bigquery_table_ID}'
            query_result = self._execute_query(query)
            result[f'{field}_val'] = query_result[column_name][0]
        return  result
    

    def get_filtered_rows_from_filter(self, filter=None, columns=[], jsonify = False):
        # HACK!  We need to write a general Filter-to-SQL function
        # and get the DB to do this, rather than paying for the bandwidth
        # to do it locally.
        # column_sql = '*' if columns is None or columns == [] else ', '.join(columns)
        # Can't pick the columns because we might need them to filter
        
        df = self.bigquery_table.get_rows()
        result = df.values.tolist()
        if filter is not None:
            rows = filter.filter(rows)
        if columns == []:
            result =  rows
            column_types = self.column_types()
        else:
            names = self.column_names()
            column_indices = [i for i in range(len(names)) if names[i] in columns]
            all_types = self.column_types()
            column_types = [all_types[i] for i in column_indices]
            result = [[row[i] for i in column_indices] for row in rows]
        return jsonifiable_rows(result, column_types) if jsonify else result