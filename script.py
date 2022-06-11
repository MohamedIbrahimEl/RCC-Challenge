from inspect import Parameter
import json
import sqlite3
from warnings import filters


with open("data.json") as f:
    data = json.load(f)

nodes = data.get('nodes')
edges = data.get('edges')

def get_previous_node(current_node):
    find_previous_node = list(filter(lambda edge:edge.get('to') == current_node, edges))
    previous_node = find_previous_node[0].get('from')
    return previous_node

def export_query(nodes):
    columns = False
    previous_node = False
    query_elements = []
    for node in nodes:
        transformObject = node['transformObject']
        if node.get('type') == 'INPUT':
            node_key = node.get('key')
            tableName = transformObject.get('tableName')
            columns = transformObject.get('fields')
            columns_str = ", ".join(columns)
            input_query = f"WITH {node_key} as (SELECT {columns_str} FROM `{tableName}`)"
            query_elements.append(input_query) 
        elif node.get('type') == 'FILTER':
            node_key = node.get('key')
            previous_node = get_previous_node(node_key)
            variable_field = transformObject.get('variable_field_name')
            operations = transformObject.get('operations')
            conditions = []
            for operation in operations:
                conditions.append(f"{variable_field} {operation['operator']} {operation['value']}")
            conditions_str = " ,".join(conditions)
            filter_query = f"{node_key} as (SELECT {columns_str} FROM {previous_node} WHERE {conditions_str})"
            query_elements.append(filter_query) 
        elif node.get('type') == 'SORT':
            node_key = node.get('key')
            previous_node = get_previous_node(node_key)
            sort_operators = []
            for condition in transformObject:
                sort_operators.append(f"{condition['target']} {condition['order']}")
            sort_options = " ,".join(sort_operators)
            sort_query = f"{node_key} as (SELECT {columns_str} FROM {previous_node} ORDER BY {sort_options})"
            query_elements.append(sort_query) 
        elif node.get('type') == 'TEXT_TRANSFORMATION':
            node_key = node.get('key')
            previous_node = get_previous_node(node_key)
            for operation in transformObject:
                column = operation.get('column')
                update_columns = []
                for col in columns:
                    if col == column:
                        update_columns.append(f"{operation.get('transformation')}(`{column}`) as `{column}`")
                    else:
                        update_columns.append(f"{col}")
            update_columns_str = ", ".join(update_columns)
            text_query = f"{node_key} as (SELECT {update_columns_str} FROM {previous_node})"
            query_elements.append(text_query)
        elif node.get('type') == 'OUTPUT':
            node_key = node.get('key')
            previous_node = get_previous_node(node_key)
            transformObjectKeys = transformObject.keys()
            output = []
            for key in transformObjectKeys:
                key_str = f'{key}'
                output.append(f"{key} {transformObject.get(key_str)}")
            output_str = " ".join(output)
            output_query = f"{node_key} as (SELECT * FROM {previous_node} {output_str})"
            query_elements.append(output_query)
    final_query = f"SELECT * FROM {previous_node};"
    query_elements.append(final_query)
    query = ",\n".join(query_elements)
    print(query)
    return query

export_query(nodes)