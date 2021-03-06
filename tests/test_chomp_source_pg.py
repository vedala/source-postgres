import unittest
from unittest.mock import patch, MagicMock
import os
from source_postgres import SourcePostgres
from source_postgres.chomp_source_pg import ChompSourceException
from source_postgres.sql_construct import construct_sql
import psycopg2

class ExtractTestCase(unittest.TestCase):
    """Tests for `extract.py`."""

    @patch.object(SourcePostgres, '__init__')
    def test_construct_connect_string(self, mock_init):
        """Is correct connect string constructed from supplied dictionary?"""

        mock_init.return_value = None

        source_pg_inst = SourcePostgres("", "")
        source_pg_inst.credentials = { 'somekey1': 'some_value', 'somekey2':'another_value'}
        expected_string="somekey1=some_value somekey2=another_value "
        self.assertEqual(expected_string, source_pg_inst.construct_connect_string())


    @patch('psycopg2.connect')
    @patch.object(SourcePostgres, 'db_initialization')
    @patch.object(SourcePostgres, '__init__')
    def test_execute_method(self, mock_init, mock_db_init, mock_pg_connect):
        """Is execute_sql on cursor called with sql_str argument?"""
        
        mock_init.return_value = None

        source = SourcePostgres()
        source.db_initialization()
        mock_cursor = MagicMock()
        source.cursor = mock_cursor
        source.execute_sql("some_sql_string")
        mock_cursor.execute.assert_called_once_with("some_sql_string")


    @patch('psycopg2.connect')
    @patch.object(SourcePostgres,
                'construct_connect_string', return_value='some_connect_string')
    @patch.object(SourcePostgres, '__init__')
    def test_db_initialization(self, mock_init, mock_cons_conn_str,
                                                          mock_pg_connect):
        """Does the method make calls to methods correctly?"""

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


    @patch.object(SourcePostgres, '__init__')
    def test_validate_config(self, mock_init):
        """Does the method raise exceptions on invalid config?"""

        mock_init.return_value = None
        source = SourcePostgres()

        source.source_config = {}
        with self.assertRaises(ChompSourceException) as ctx:
            source.validate_config()
        self.assertEqual("Key 'table' not present.", str(ctx.exception))

        source.source_config = {'table': 'some_table'}
        with self.assertRaises(ChompSourceException) as ctx:
            source.validate_config()
        self.assertEqual("Key 'columns' not present.", str(ctx.exception))

        source.source_config = {'table': 'some_table', 'columns': []}
        with self.assertRaises(ChompSourceException) as ctx:
            source.validate_config()
        self.assertEqual("Value for 'columns' key is empty.", str(ctx.exception))

        source.source_config = {'table': 'some_table', 'columns': ['col1', 'col2']}
        self.assertEqual(None, source.validate_config())


    @patch.object(SourcePostgres, '__init__')
    def test_cleanup(self, mock_init):
        """Are close() methods called on objects in variables - cursor and connection?"""

        mock_init.return_value = None

        source = SourcePostgres()
        source.cursor = MagicMock()
        source.connection = MagicMock()

        mock_cursor_close = MagicMock(return_value="aa")
        mock_conn_close = MagicMock(return_value="bb")
        source.cursor.close = mock_cursor_close
        source.connection.close = mock_conn_close

        source.cleanup()
        mock_cursor_close.assert_called_once_with()
        mock_conn_close.assert_called_once_with()


    @patch.object(SourcePostgres, '__init__')
    def test_get_batch(self, mock_init):
        """Is fetchmany method on cursor called with itersize argument?"""

        mock_init.return_value = None

        source = SourcePostgres()
        source.itersize = 8765
        source.cursor = MagicMock()
        mock_cursor_fetchmany = MagicMock(
                            return_value="this is fetchmany return value")
        source.cursor.fetchmany = mock_cursor_fetchmany

        get_batch_retval = source.get_batch()
        mock_cursor_fetchmany.assert_called_once_with(8765)
        self.assertEqual("this is fetchmany return value", get_batch_retval)


    @patch.object(SourcePostgres, '__init__')
    def test_init_call(self, mock_init):
        """Is the __init__ method called with expected arguments?"""

        mock_init.return_value = None

        credentials = {"dbname": "db_one", "user": "chompuser1"}
        source_config = {
            "table" : "customers",
            "columns": [
                 "first_name", "last_name", "zip"
             ]
        }

        source = SourcePostgres(credentials, source_config)
        mock_init.assert_called_once_with(credentials, source_config)


    @patch('psycopg2.connect')
    @patch.object(SourcePostgres, 'construct_connect_string')
    @patch.object(SourcePostgres, 'execute_sql')
    @patch('source_postgres.chomp_source_pg.construct_sql',
                                            return_value="some_sql_str")
    @patch.object(SourcePostgres, 'validate_config', return_value=None)
    def test_init_method(self, mock_validate_config, mock_construct_sql,
                      mock_execute_sql, mock_cons_conn_str, mock_pg_connect):
        """Does the __init__ method call other methods as expected?"""

        class MyCursor(object):
            def __init__(self, cursor_name):
                self.itersize = 5678

        class MyConnection(object):
            def cursor(self, cursor_name):
                return MyCursor(cursor_name)

        mock_pg_connect.return_value = MyConnection()

        source_config = {
            "table" : "customers",
            "columns": [
                 "first_name", "last_name", "zip"
             ]
        }

        source = SourcePostgres("", source_config)
        mock_validate_config.assert_called_once_with()
        mock_construct_sql.assert_called_once_with(source_config)
        mock_execute_sql.assert_called_once_with("some_sql_str")
        self.assertEqual(5678, source.itersize)

if __name__ == "__main__":
    unittest.main()
