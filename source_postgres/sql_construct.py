def construct_sql(table, columns):
    col_str = ", ".join(columns)
    table = self.source_config['table']
    sql_str = f"SELECT {col_str} FROM {table}"
    return sql_str
