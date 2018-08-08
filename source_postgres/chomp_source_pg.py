import psycopg2
from .sql_construct import construct_sql

class ChompSourceException(Exception):
    pass

class SourcePostgres(object):

    def __init__(self, credentials, source_config):
        self.credentials = credentials
        self.source_config = source_config
        self.validate_config()
        self.db_initialization()
        sql_str = construct_sql(source_config['table'], source_config['columns'])
        self.execute_sql(sql_str)
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

    def execute_sql(self, sql_str):
        self.cursor.execute(sql_str)

    def construct_connect_string(self):
        connect_string = ""
        for (k, v) in self.credentials.items():
            connect_string += f"{k}={v} "
        return connect_string
