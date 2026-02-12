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
from sqlglot import exp, parse_one, ErrorLevel

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


def cleanup_table_names(schemas: dict) -> dict:
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


def _replace_illegal_character_with_token(name: str) -> str:
  for char, token in CHAR_MAPPING.items():
    if char in name:
      name = name.replace(char, token)
  return name


def _starts_with_known_extension(name: str) -> bool:
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


def fix_aggregate_query_projection(sql: sqlglot.Expression) -> sqlglot.Expression:
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


def transpile_sql(sql, source_dialect, target_dialect, schemas=None) -> str:
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
