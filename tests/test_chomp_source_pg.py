import unittest
from unittest.mock import patch, MagicMock
import os
from source_postgres import SourcePostgres
import psycopg2

class ExtractTestCase(unittest.TestCase):
    """Tests for `extract.py`."""

    def test_first(self):
        """Is ?"""

        credentials = {"dbname": "db_one", "user": "chompuser1"}
        source_config = {
            "table" : "customers",
            "columns": [
                 "first_name", "last_name", "zip"
             ]
        }
        source = SourcePostgres(credentials, source_config)
        self.assertEqual(2000, source.itersize)

    @patch.object(SourcePostgres, '__init__')
    def test_construct_connect_string(self, mock_init):
        """Is correct connect string constructed from supplied dictionary?"""

        mock_init.return_value = None

        source_pg_inst = SourcePostgres("", "")
        source_pg_inst.credentials = { 'somekey1': 'some_value', 'somekey2':'another_value'}
        expected_string="somekey1=some_value somekey2=another_value "
        self.assertEqual(expected_string, source_pg_inst.construct_connect_string())


    @patch.object(SourcePostgres, '__init__')
    @patch.object(SourcePostgres, 'execute_sql')
    def test_execute_sql(self, mock_execute_sql, mock_init):
        """Is execute_sql on cursor called with sql_str argument?"""
        
        mock_init.return_value = None
        source = SourcePostgres()
        source.execute_sql("some_sql_string")
        mock_execute_sql.assert_called_once_with("some_sql_string")


    @patch('psycopg2.connect')
    @patch.object(SourcePostgres,
                'construct_connect_string', return_value='some_connect_string')
    @patch.object(SourcePostgres, '__init__')
    def test_db_initialization(self, mock_init, mock_cons_conn_str,
                                                          mock_pg_connect):
        """Is execute on cursor called with sql_str argument?"""

        class MyConnection(object):
            def cursor(self, cursor_name):
                return cursor_name

        mock_init.return_value = None
        mock_pg_connect.return_value = MyConnection()
        source = SourcePostgres()
        source.db_initialization()
        mock_cons_conn_str.assert_called_once_with()
        mock_pg_connect.assert_called_once_with("some_connect_string")
        self.assertEqual("chompetl_named_cursor", source.cursor)

if __name__ == "__main__":
    unittest.main()
