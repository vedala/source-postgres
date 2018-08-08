import unittest
import os
from source_postgres import construct_sql

class SqlConstructTestCase(unittest.TestCase):
    """Tests for `sql_construct.py`."""

    def get_file_contents(filename):
        try:
            with open(filename) as f:
                contents = f.read()
        except FileNotFoundError:
            print(f'Error, file "{filename}" does not exist.', file=sys.stderr)
            raise
    
        return contents

    def test_construct_basic_sql(self):
        """Is a basic sql constructed?"""

        credentials = {"dbname": "db_one", "user": "chompuser1"}
        source_config = get_file_contents("config_1001.json")
        expected_sql = "SELECT first_name, last_name, zip FROM customers"
        table = source_config['table']
        columns = source_config['columns']
        constructed_sql = construct_sql(table, columns)
        self.assertEqual(expected_sql, constructed_sql)

if __name__ == "__main__":
    unittest.main()
