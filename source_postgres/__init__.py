name = "source_postgres"

import psycopg2

class ChompSourceException(Exception):
    pass

class SourcePostgres(object):

    def __init__(self, credentials, source_config):
        self.credentials = credentials
        self.source_config = source_config
        self.curr_batch = 0
        self.validate_config()
        self.db_initialization() # connect to db # create cursor
        sql_str = self.construct_sql() # construct sql
        self.execute_sql(sql_str) # execute sql
        self.itersize = self.cursor.itersize

    def get_batch(self):
        return self.cursor.fetchmany(self.itersize)

    def cleanup(self):
        self.cursor.close()
        self.connection.close()

    def validate_config(self):
        if 'table' not in self.source_config:
            raise ChompSourceException("Key 'table' not present.")
        elif 'columns' not in self.source_config:
            raise ChompSourceException("Key 'columns' not present.")
        elif len(self.source_config['columns']) == 0:
            raise ChompSourceException("Value for 'columns' key is empty.")

    def db_initialization(self):
        connect_string = self.construct_connect_string()
        self.connection = psycopg2.connect(connect_string)
        self.cursor = self.connection.cursor('chompetl_named_cursor')

    def construct_sql(self):
        col_str = ", ".join(self.source_config['columns'])
        table = self.source_config['table']
        sql_str = f"SELECT {col_str} FROM {table}"
        return sql_str

    def execute_sql(self, sql_str):
        self.cursor.execute(sql_str)

    def construct_connect_string(self):
        connect_string = ""
        for (k, v) in self.credentials.items():
            connect_string += f"{k}={v} "
        return connect_string
