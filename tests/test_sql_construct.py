import unittest
from unittest.mock import patch
import os
import json
from source_postgres import construct_sql
from source_postgres.sql_construct import build_operator, build_operand, construct_expression

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
        constructed_sql = construct_sql(source_config)
        self.assertEqual(expected_sql, constructed_sql)

    def test_construct_where_clause(self):
        """Is a sql with where constructed given source configuration?"""

        source_config_json = get_file_contents("config_1002.json")
        source_config = json.loads(source_config_json)
        expected_sql = \
            "SELECT col501, col502, col503 FROM tab5 " \
            "WHERE col503 = 1"
        constructed_sql = construct_sql(source_config)
        self.assertEqual(expected_sql, constructed_sql)

    def test_build_operator_with_eq(self):
        """Is an equal sign returned on calling build_operator with 'eq'?"""

        self.assertEqual("=", build_operator("eq"))

    def test_build_operator_invalid_operator(self):
        """Does it raise expcetion if invalid operator supplied?"""

        with self.assertRaises(Exception) as ctx:
            build_operator("no_op")

        self.assertEqual("Invalid operator", str(ctx.exception))

    def test_build_operand_type_column(self):
        """Is an operand of column type constructed correctly?"""

        operand_dict = { 'operand_type': 'column', 'operand': 'first_name', }

        expected_string = "first_name"
        self.assertEqual(expected_string, build_operand(operand_dict))

    def test_build_operand_type_literal_int(self):
        """Is an operand of string literal type constructed correctly?"""

        operand_dict = { 'operand_type': 'literal', 'operand': 'a_first_name', }
        self.assertEqual("'a_first_name'", build_operand(operand_dict))

    def test_build_operand_type_literal_string(self):
        """Is an operand of int literal type constructed correctly?"""

        operand_dict = { 'operand_type': 'literal', 'operand': 5678, }
        self.assertEqual("5678", build_operand(operand_dict))

    def test_build_operand_type_unsupported_literal_type(self):
        """Does it raise an exception on supplying unsupported literal type?"""

        operand_dict = { 'operand_type': 'literal', 'operand': None, }
        with self.assertRaises(Exception) as ctx:
            build_operand(operand_dict)

        self.assertEqual("Unsupported literal type encountered", str(ctx.exception))

    def test_build_operand_type_invalid_operand_type(self):
        """Does it raise an exception on supplying invalid operand_type?"""

        operand_dict = { 'operand_type': 'invalid_value', 'operand': 'aaa', }
        with self.assertRaises(Exception) as ctx:
            build_operand(operand_dict)

        self.assertEqual("Invalid operand_type specified", str(ctx.exception))

    @patch('source_postgres.sql_construct.build_operator', return_value="=")
    @patch('source_postgres.sql_construct.build_operand',
                                side_effect=["first_name", "'a_first_name'"])
    def test_construct_expression(self, mock_build_operand, mock_build_operator):
        """Is correct expression constructed?"""

        where_dict = { 'operand_1': 'some_value', 'operand_2': 'some_value',
                        'operator': 'some_value' }
        expected_string = "first_name = 'a_first_name'"
        self.assertEqual(expected_string, construct_expression(where_dict))

if __name__ == "__main__":
    unittest.main()
