import unittest
import os
import json
from source_postgres import construct_sql

def get_file_contents(filename):
    try:
        with open(filename) as f:
            contents = f.read()
    except FileNotFoundError:
        print(f'Error, file "{filename}" does not exist.', file=sys.stderr)
        raise

    return contents

class SqlConstructTestCase(unittest.TestCase):
    """Tests for `sql_construct.py`."""


    def test_construct_basic_sql(self):
        """Is a basic sql constructed given source configuration?"""

        source_config_json = get_file_contents("config_1001.json")
        source_config = json.loads(source_config_json)
        expected_sql = "SELECT first_name, last_name, zip FROM customers"
        table = source_config['table']
        columns = source_config['columns']
        constructed_sql = construct_sql(table, columns)
        self.assertEqual(expected_sql, constructed_sql)

if __name__ == "__main__":
    unittest.main()
