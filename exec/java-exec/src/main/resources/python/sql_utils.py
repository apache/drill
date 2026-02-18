# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
SQL utility functions for Apache Drill.
Loaded by SqlTranspiler via GraalPy for SQL dialect transpilation
and other SQL manipulation tasks.
"""

import json
import sqlglot
from sqlglot import exp, parse_one


EXTENSIONS = [
  "3g2", "3gp", "accdb", 'mdb', 'mdx', 'ai', 'arw',
  'avi',
  'avro',
  'bmp',
  'cr2',
  'crw',
  'csv',
  'csvh',
  'dng',
  'eps',
  'epsf',
  'epsi',
  'gif',
  'h5',
  'httpd',
  'ico',
  'jpe',
  'jpeg',
  'jpg',
  'json',
  'log',
  'ltsv',
  'm4a',
  'm4b',
  'm4p',
  'm4r',
  'm4v',
  'mov',
  'mp4',
  'nef',
  'orf',
  'parquet',
  'pcap',
  'pcapng',
  'pcx',
  'pdf',
  'png',
  'psd',
  'raf',
  'rw2',
  'rwl',
  'sav',
  'sas7bdat',
  'seq',
  'shp',
  'srw',
  'syslog',
  'tbl',
  'tif',
  'tiff',
  'tsv',
  'wav',
  'wave',
  'webp',
  'x3f',
  'xlsx',
  'xml',
  'zip'
]

CHAR_MAPPING = {
  " ": "__SPACE__",
  "-": "__DASH__",
  ",": "__COMMA__",
  ":": "__COLON__",
  ";": "__SEMICOLON__",
  "(": "__RPAREN__",
  ")": "__LPAREN__",
  "1": "__ONE__",
  "2": "__TWO__",
  "3": "__THREE__",
  "4": "__FOUR__",
  "5": "__FIVE__",
  "6": "__SIX__",
  "7": "__SEVEN__",
  "8": "__EIGHT__",
  "9": "__NINE__",
  "0": "__ZERO__"
}


def json_parse(json_string):
  """Parse a JSON string into a Python object.
  Used by the Java SqlTranspiler to convert JSON strings to Python dicts/lists."""
  return json.loads(json_string)


def format_sql(sql):
  """Pretty-print a SQL string using sqlglot.

  Args:
      sql: The SQL query string.

  Returns:
      The formatted SQL string, or the original if parsing fails.
  """
  try:
    return parse_one(sql, read="drill").sql(dialect="drill", pretty=True)
  except Exception:
    return sql


def cleanup_table_names(schemas):
  """
  Ensures that the table names do not have any illegal characters in them. Additionally,
  views in Drill are treated as files with the extensions .view.drill.  The view extensions are
  not necessary for executing a query and should be removed at this point.
  :param schemas: The raw input schema.
  :return: schema with cleaned table and column names
  """

  for i, schema in enumerate(schemas):
    # First check the table name
    schema['name'] = _replace_illegal_character_with_token(schema['name'])

    # Now go through the tables and columns
    for table in schema['tables']:
      # Remove view file extensions
      table['name'] = table['name'].replace('.view.drill', '')
      table['name'] = _replace_illegal_character_with_token(table['name'])

      # Now check the columns
      columns = table['columns']
      for j, column in enumerate(columns):
        columns[j] = _replace_illegal_character_with_token(column)
    schemas[i] = schema
  return schemas


def _replace_illegal_character_with_token(name):
  for char, token in CHAR_MAPPING.items():
    if char in name:
      name = name.replace(char, token)
  return name


def _get_column_name(node):
  if node.table:
    return f"{node}"
  return f"{node.alias_or_name}"


def _starts_with_known_extension(name):
  for ext in EXTENSIONS:
    if name.startswith(ext):
      return True
  return False


def remove_tokens(sql):
  """
  This function removes the tokens to represent illegal characters in SQL.
  :param sql: The input SQL string with tokens
  :return: The output SQL query with the tokens removed.
  """
  for char, token in CHAR_MAPPING.items():
    if token in sql:
      sql = sql.replace(token, char)

  return sql


def is_star_query(sql):
  """
  Returns true if the query is a star query. Note that SELECT *, foo would be
  considered a star query.
  :param sql: A sql query
  :return: True if the input string is a star query, false if not.
  """
  if not isinstance(sql, exp.Select):
    query = parse_one(sql)
  else:
    query = sql

  star_query = query.find_all(exp.Star)

  for column in star_query:
    if column.parent_select != query:
      return False
    return True

  return False


def replace_star_with_columns(sql, column_list):
  """
  This function will replace a star query with the list of columns.
  :param sql: The input query which should be a star query.
  :param column_list:  A dictionary where the keys are field names.
  :return: An updated query with the star replaced with field names
  """

  columns = list(column_list.keys())

  # If there are additional columns other than the star, add them to the column list
  if len(sql.named_selects) > 1:
    for select in sql.named_selects:
      if select != '*':
        columns.append(select)

  first_column = True
  for column in columns:
    # Always escape the columns
    column = f"`{column}`"
    if first_column:
      sql.select(column, append=False, copy=False, dialect='drill')
      first_column = False
    else:
      sql = sql.select(column, append=True, copy=False, dialect='drill')
  return sql


def ensure_full_table_names(sql, schemas):
    """
    Open AI does not always return the correct table names. This function fixes that by
    cleaning up the table names.
    :param sql: Original SQL query from OpenAI
    :param schemas: The Schemas as provided from DataDistillr
    :return: A query with the full table names.
    """

    parsed_query = parse_one(sql, read="drill")

    # Get a list of tables in the query
    tables = parsed_query.find_all(exp.Table)
    table_list = []
    table_to_plugin_map = {}
    for table in tables:
        table_list.append(table.name)
        table_list.append(table)

    # Make a kv structure of tables
    for schema in schemas:
        plugin_name = schema['name']
        for table in schema['tables']:
            table_name = table['name']

            # If the table is a file, add the first part to the mapping as OpenAI tends to ignore
            # the part after the dot.  This is obviously important for files.
            if table_name.count('.') > 0:
                table_parts = table_name.split('.')

                # Check for files
                if _starts_with_known_extension(table_parts[1]):
                    table_to_plugin_map[table_name] = sqlglot.expressions.table_(db=plugin_name, table=table_name,
                                                                                 alias=table_parts[0])

                if len(table_list) > 1:
                    table_to_plugin_map[table_parts[0]] = sqlglot.expressions.table_(db=plugin_name, table=table_name,
                                                                                     alias=table_parts[0])
                else:
                    table_to_plugin_map[table_parts[0]] = f"{plugin_name}.{table_name}"

                # Add plugin name to replacement map
                table_name_with_plugin = f"{plugin_name}.{table_parts[0]}"

                # Check to see whether the file is a compound file.  If so, replace it with a table() function
                if len(table_list) > 1:
                    # If there are multiple tables in the query, add an alias to files
                    table_to_plugin_map[table_name_with_plugin] = sqlglot.expressions.table_(db=plugin_name,
                                                                                             table=table_name,
                                                                                             alias=table_parts[0])
                else:
                    table_to_plugin_map[table_name_with_plugin] = f"{plugin_name}.{table_name}"

            elif not table_name and plugin_name.count('.') > 0:
                # For some databases, the table name doesn't get populated but it is in the plugin name
                table_to_plugin_map[plugin_name.split('.')[1]] = plugin_name
            else:
                table_to_plugin_map[table_name] = plugin_name + "." + table_name

    # Now find the full table name from the provided schema
    result = exp.replace_tables(parsed_query, table_to_plugin_map)
    result = fix_aggregate_query_projection(result)
    return result.sql(dialect="drill", pretty=True)


def fix_aggregate_query_projection(sql):
  """
  LLMs seem to generate aggregate queries that project columns which are not in the
  GROUP BY clause.  Drill does not allow this, although some SQL engines do.
  :param sql:  The incoming SQL query.
  :return: A query with all the projected fields in the GROUP BY clause.
  """

  group = sql.args.get('group')

  # If the query is not an aggregate query, return the original query without modification
  if group is None:
    return sql

  # For now, we only want scalar columns and not function calls.
  for column in sql.selects:
    if isinstance(column, exp.Column) and column not in group.expressions:
      sql.group_by(column, append=True, copy=False)

  return sql


def transpile_sql(sql, source_dialect, target_dialect, schemas=None):
    """Transpile SQL from one dialect to another.

    Args:
        sql: The SQL string to transpile.
        source_dialect: The source dialect (e.g. 'mysql', 'postgres').
        target_dialect: The target dialect (e.g. 'drill').
        schemas: Optional list of schema metadata dicts for table name resolution.

    Returns:
        The transpiled SQL string, or the original SQL if transpilation fails.
    """
    try:
        result = sqlglot.transpile(
            sql,
            read=source_dialect,
            write=target_dialect,
            identify=True,
        )
        transpiled_sql = result[0] if result else sql

        if schemas and len(schemas) > 0:
            transpiled_sql = ensure_full_table_names(transpiled_sql, schemas)

        return transpiled_sql
    except Exception:
        return sql


def convert_data_type(sql, column_name, data_type, column_list=None):
  """Convert a column's data type by wrapping it in a CAST expression.
  Uses sqlglot AST manipulation for robust SQL transformation.

  Args:
      sql: The SQL query string.
      column_name: The column to convert.
      data_type: The target SQL data type (e.g. 'INTEGER', 'VARCHAR').
      column_list: Optional dict mapping column names to types (for star query expansion).

  Returns:
      The transformed SQL string.
  """
  parsed_query = parse_one(sql, read="drill")

  if is_star_query(parsed_query):
    if not column_list:
      return sql
    parsed_query = replace_star_with_columns(parsed_query, column_list)

  column_nodes = parsed_query.find_all(exp.Column)
  for column in column_nodes:
    if column.alias_or_name == column_name:
      if isinstance(column, exp.Column):
        alias = f"`{column_name}`"
        if isinstance(column.parent, exp.Alias):
          # Column already has an alias — preserve it
          existing_alias = column.parent.alias
          cast_expr = parse_one(f"CAST({_get_column_name(column)} AS {data_type}) AS `{existing_alias}`", read="drill")
          column.parent.replace(cast_expr)
        elif isinstance(column.parent, exp.Func):
          # Column is inside a function — wrap the outermost function
          parent = column.parent
          while isinstance(parent.parent, exp.Func):
            parent = parent.parent

          if isinstance(parent.parent, exp.Alias):
            existing_alias = parent.parent.alias
            cast_expr = parse_one(f"CAST({parent} AS {data_type}) AS `{existing_alias}`", read="drill")
            parent.parent.replace(cast_expr)
          else:
            cast_expr = parse_one(f"CAST({parent} AS {data_type}) AS {alias}", read="drill")
            parent.replace(cast_expr)
        else:
          # Simple column with no alias — add original name as alias
          cast_expr = parse_one(f"CAST({_get_column_name(column)} AS {data_type}) AS {alias}", read="drill")
          column.replace(cast_expr)
  return parsed_query.sql(dialect="drill", pretty=True)


def convert_data_type_raw(sql, column_name, data_type, columns_json=None):
  """Convert a column's data type. Entry point for Java/GraalPy.

  Args:
      sql: The SQL query string.
      column_name: The column to convert.
      data_type: The target SQL data type (e.g. 'INTEGER', 'VARCHAR').
      columns_json: Optional JSON string of column name -> type mapping (for star queries).

  Returns:
      The transformed SQL string.
  """
  columns = json.loads(columns_json) if columns_json else None
  return convert_data_type(sql, column_name, data_type, columns)

def change_time_grain(sql: str, column_name: str, time_grain: str, column_list: list[str]):
    """
    Modifies the SQL query to change the time grain of a specified column by replacing it with a function call.
    The function handles different cases in the query structure, including simple columns, aliased columns,
    columns within functions, and subqueries. If the column has no alias, the original column name is used as
    the alias for the transformed column.

    Parameters:
        sql (str): The original SQL query string to be modified.
        column_name (str): The name of the column whose time grain needs to be changed.
        time_grain (str): The time grain transformation to be applied to the column.
        column_list (list[str]): A list of all column names in the table, used when the query contains a wildcard (*).

    Returns:
        dict: A dictionary containing the modified SQL query string. The key is:
            - "sql": The new SQL query string with the updated time grain transformation.

    Raises:
        None
    """

    function_name = "DATE_TRUNC"
    time_grain = time_grain.upper()
    parsed_query = parse_one(sql, read="drill")

    # Should never be a star query
    if is_star_query(parsed_query):
      # replace_star_with_columns expects a dict; convert list to dict if needed
      if isinstance(column_list, list):
        column_list = {col: "VARCHAR" for col in column_list}
      parsed_query = replace_star_with_columns(parsed_query, column_list)

    column_nodes = parsed_query.find_all(exp.Column)
    for column in column_nodes:
      if column.alias_or_name == column_name:
        # There are several cases
        # 1.  The column is a simple column with no alias.
        # 2.  The column is a simple column with an alias.
        # 3.  The column is a function.
        # 4.  The column is already a DATE_TRUNC function
        # 5.  The column is a subquery.
        if isinstance(column, exp.Column):
          if isinstance(column.parent, exp.Alias):
            # Case 2: Column already has an alias.  In this case, we reuse the alias.
            updated_node = parse_one(function_name + f"({time_grain}, {_get_column_name(column)})")
            column.replace(updated_node)

          # Case 4:  Existing DATE_TRUNC
          elif isinstance(column.parent, exp.DateTrunc):
            parent = column.parent
            parent.set("unit", exp.Literal.string(time_grain))

          elif isinstance(column.parent, exp.Func):
            # Case 3: The column is in a function.
            # Recurse out of the current node to find the outermost parent node that is a function
            parent = column.parent
            while isinstance(parent.parent, exp.Func):
              parent = parent.parent

            if isinstance(parent.parent, exp.Alias):
              updated_node = parse_one(f"{function_name}({time_grain}, {parent})")
            else:
              updated_node = parse_one(f"{function_name}({time_grain}, {parent}) AS {column_name}")
            parent.replace(updated_node)

          else:
            # Case 1: Column has no alias.  In this case, we add the original column name as an alias
            updated_node = parse_one(
              f"{function_name}({time_grain}, {_get_column_name(column)}) AS {column.alias_or_name}")
            column.replace(updated_node)
    return {
      "sql": parsed_query.sql(dialect="drill", pretty=True, normalize_functions="lower")
    }


def change_time_grain_raw(sql, column_name, time_grain, columns_json=None):
  """Entry point for Java/GraalPy. Accepts a JSON string for the column list.

  Args:
      sql: The SQL query string.
      column_name: The temporal column to transform.
      time_grain: The time grain (e.g. 'MONTH', 'YEAR').
      columns_json: Optional JSON array string of column names (for star queries).

  Returns:
      The transformed SQL string.
  """
  columns = json.loads(columns_json) if columns_json else []
  result = change_time_grain(sql, column_name, time_grain, columns)
  return result["sql"]
