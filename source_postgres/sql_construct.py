def construct_sql(source_config):
    table = source_config['table']
    columns = source_config['columns']
    col_str = ", ".join(columns)
    sql_str = f"SELECT {col_str} FROM {table}"
    where_str = ""
    if 'where' in source_config:
        where_str += " " + construct_where(source_config['where'])

    sql_str += where_str
    return sql_str

def construct_where(where_dict):
    where_str = "WHERE "
    subclause_type = where_dict['subclause_type']
    if subclause_type == "expression":
        subclause_str = construct_expression(where_dict)
    else:
        raise Exception("Invalid subclause_type")

    where_str += subclause_str
    return where_str

def construct_expression(where_dict):
    expression_str = ""
    operand_1 = where_dict['operand_1']
    operator = where_dict['operator']
    operand_2 = where_dict['operand_2']
    expression_str += build_operand(operand_1)
    expression_str += " " + build_operator(operator) + " "
    expression_str += build_operand(operand_2)
    return expression_str

def build_operand(operand_dict):
    operand_str = ""
    operand_type = operand_dict['operand_type']
    operand = operand_dict['operand']
    if operand_type == "column":
        operand_str = operand_dict['operand']
    elif operand_type == "literal":
        if isinstance(operand, int):
            operand_str += str(operand)
        elif isinstance(operand, str):
            operand_str += f"'{operand}'"
        else:
            raise Exception("Unsupported literal type encountered")
    else:
        raise Exception("Invalid operand_type specified")

    return operand_str

def build_operator(operator_str):
    if operator_str == "eq":
        return "="
    else:
        raise Exception("Invalid operator")
