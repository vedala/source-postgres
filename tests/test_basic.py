import unittest
import os
from source_postgres import SourcePostgres

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

if __name__ == "__main__":
    unittest.main()
