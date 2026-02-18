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
Comprehensive tests for sql_utils.py — the Python SQL manipulation functions
used by Apache Drill's GraalPy + sqlglot integration.
"""

import copy
import json
import pytest
from sqlglot import parse_one

from sql_utils import (
    CHAR_MAPPING,
    EXTENSIONS,
    json_parse,
    format_sql,
    cleanup_table_names,
    remove_tokens,
    _replace_illegal_character_with_token,
    _starts_with_known_extension,
    is_star_query,
    replace_star_with_columns,
    ensure_full_table_names,
    fix_aggregate_query_projection,
    transpile_sql,
    convert_data_type,
    convert_data_type_raw,
    change_time_grain,
    change_time_grain_raw,
)


# ===========================================================================
# json_parse
# ===========================================================================

class TestJsonParse:
    def test_parse_object(self):
        result = json_parse('{"key": "value"}')
        assert result == {"key": "value"}

    def test_parse_array(self):
        result = json_parse('[1, 2, 3]')
        assert result == [1, 2, 3]

    def test_parse_string(self):
        result = json_parse('"hello"')
        assert result == "hello"

    def test_parse_number(self):
        result = json_parse('42')
        assert result == 42

    def test_parse_nested(self):
        result = json_parse('{"a": [1, {"b": true}]}')
        assert result == {"a": [1, {"b": True}]}

    def test_invalid_json_raises(self):
        with pytest.raises(json.JSONDecodeError):
            json_parse("{bad json")


# ===========================================================================
# format_sql
# ===========================================================================

class TestFormatSql:
    def test_format_simple_select(self):
        result = format_sql("SELECT a, b FROM t")
        assert "SELECT" in result
        assert "FROM" in result

    def test_format_multi_clause(self):
        result = format_sql("SELECT a FROM t WHERE a > 1 ORDER BY a")
        assert "WHERE" in result
        assert "ORDER BY" in result

    def test_invalid_sql_returns_original(self):
        bad = "NOT VALID SQL %%% @@"
        result = format_sql(bad)
        assert result == bad


# ===========================================================================
# _replace_illegal_character_with_token / remove_tokens round-trip
# ===========================================================================

class TestCharacterTokenisation:
    def test_replace_space(self):
        assert _replace_illegal_character_with_token("hello world") == "hello__SPACE__world"

    def test_replace_dash(self):
        assert _replace_illegal_character_with_token("my-table") == "my__DASH__table"

    def test_replace_digit(self):
        assert _replace_illegal_character_with_token("table1") == "table__ONE__"

    def test_replace_parens(self):
        result = _replace_illegal_character_with_token("col(a)")
        assert "__RPAREN__" in result
        assert "__LPAREN__" in result

    def test_no_illegal_chars(self):
        assert _replace_illegal_character_with_token("clean_name") == "clean_name"

    def test_round_trip(self):
        """cleanup then remove_tokens should recover the original name."""
        original = "my table-1 (test)"
        tokenized = _replace_illegal_character_with_token(original)
        restored = remove_tokens(tokenized)
        assert restored == original

    def test_remove_tokens_no_tokens(self):
        assert remove_tokens("plain_sql") == "plain_sql"


# ===========================================================================
# cleanup_table_names
# ===========================================================================

class TestCleanupTableNames:
    def test_replaces_spaces_in_table_names(self):
        schemas = [{
            "name": "my schema",
            "tables": [{
                "name": "my table",
                "columns": ["col a"]
            }]
        }]
        result = cleanup_table_names(copy.deepcopy(schemas))
        assert "__SPACE__" in result[0]["name"]
        assert "__SPACE__" in result[0]["tables"][0]["name"]
        assert "__SPACE__" in result[0]["tables"][0]["columns"][0]

    def test_strips_view_drill_extension(self):
        schemas = [{
            "name": "dfs",
            "tables": [{
                "name": "myview.view.drill",
                "columns": ["id"]
            }]
        }]
        result = cleanup_table_names(copy.deepcopy(schemas))
        assert ".view.drill" not in result[0]["tables"][0]["name"]
        assert result[0]["tables"][0]["name"] == "myview"

    def test_round_trip_cleanup_remove(self):
        schemas = [{
            "name": "s-1",
            "tables": [{
                "name": "t 2",
                "columns": ["col-a"]
            }]
        }]
        cleaned = cleanup_table_names(copy.deepcopy(schemas))
        restored_table = remove_tokens(cleaned[0]["tables"][0]["name"])
        assert restored_table == "t 2"


# ===========================================================================
# _starts_with_known_extension
# ===========================================================================

class TestStartsWithKnownExtension:
    @pytest.mark.parametrize("ext", ["csv", "parquet", "json", "xlsx", "avro", "tsv"])
    def test_known_extensions(self, ext):
        assert _starts_with_known_extension(ext) is True

    def test_known_extension_with_suffix(self):
        assert _starts_with_known_extension("csv.gz") is True

    def test_unknown_extension(self):
        assert _starts_with_known_extension("foo") is False

    def test_empty_string(self):
        assert _starts_with_known_extension("") is False


# ===========================================================================
# is_star_query
# ===========================================================================

class TestIsStarQuery:
    def test_select_star(self):
        assert is_star_query("SELECT * FROM t") is True

    def test_select_star_with_extra_columns(self):
        assert is_star_query("SELECT *, foo FROM t") is True

    def test_select_columns(self):
        assert is_star_query("SELECT col1, col2 FROM t") is False

    def test_subquery_star_not_detected(self):
        """Star inside a subquery should not make the outer query a star query."""
        assert is_star_query("SELECT a FROM (SELECT * FROM t) sub") is False

    def test_accepts_parsed_expression(self):
        expr = parse_one("SELECT * FROM t")
        assert is_star_query(expr) is True


# ===========================================================================
# replace_star_with_columns
# ===========================================================================

class TestReplaceStarWithColumns:
    def test_expand_star(self):
        query = parse_one("SELECT * FROM t", read="drill")
        columns = {"id": "INT", "name": "VARCHAR", "age": "INT"}
        result = replace_star_with_columns(query, columns)
        sql = result.sql(dialect="drill")
        for col in columns:
            assert col in sql
        assert "*" not in sql

    def test_preserve_additional_columns(self):
        query = parse_one("SELECT *, extra FROM t", read="drill")
        columns = {"id": "INT", "name": "VARCHAR"}
        result = replace_star_with_columns(query, columns)
        sql = result.sql(dialect="drill")
        assert "extra" in sql
        assert "*" not in sql


# ===========================================================================
# ensure_full_table_names
# ===========================================================================

class TestEnsureFullTableNames:
    def test_prefix_bare_table(self):
        schemas = [{
            "name": "dfs",
            "tables": [{"name": "users", "columns": ["id", "name"]}]
        }]
        result = ensure_full_table_names("SELECT id FROM users", schemas)
        assert "dfs" in result.lower()

    def test_file_based_table(self):
        schemas = [{
            "name": "dfs",
            "tables": [{"name": "data.csv", "columns": ["id"]}]
        }]
        result = ensure_full_table_names("SELECT id FROM data", schemas)
        assert "dfs" in result.lower()

    def test_compound_file_with_alias(self):
        schemas = [{
            "name": "dfs",
            "tables": [{"name": "data.parquet", "columns": ["id", "value"]}]
        }]
        result = ensure_full_table_names("SELECT id FROM data", schemas)
        assert "dfs" in result.lower()


# ===========================================================================
# fix_aggregate_query_projection
# ===========================================================================

class TestFixAggregateQueryProjection:
    def test_adds_missing_group_by_column(self):
        query = parse_one("SELECT name, COUNT(*) FROM t GROUP BY name", read="drill")
        result = fix_aggregate_query_projection(query)
        sql = result.sql(dialect="drill")
        assert "GROUP BY" in sql.upper()
        assert "name" in sql.lower()

    def test_skips_function_columns(self):
        """Aggregate function columns should not be added to GROUP BY."""
        query = parse_one("SELECT name, SUM(amount) FROM t GROUP BY name", read="drill")
        result = fix_aggregate_query_projection(query)
        sql = result.sql(dialect="drill")
        assert "SUM" in sql.upper()

    def test_no_op_on_non_aggregate(self):
        query = parse_one("SELECT id, name FROM t", read="drill")
        result = fix_aggregate_query_projection(query)
        sql = result.sql(dialect="drill")
        assert "GROUP BY" not in sql.upper()

    def test_adds_projected_column_missing_from_group_by(self):
        query = parse_one(
            "SELECT department, city, COUNT(*) FROM t GROUP BY department",
            read="drill"
        )
        result = fix_aggregate_query_projection(query)
        sql = result.sql(dialect="drill").upper()
        assert "CITY" in sql
        assert "GROUP BY" in sql


# ===========================================================================
# transpile_sql
# ===========================================================================

class TestTranspileSql:
    def test_mysql_to_drill(self):
        result = transpile_sql(
            "SELECT IFNULL(a, 0) FROM t",
            "mysql", "drill"
        )
        # Drill uses COALESCE or NVL instead of IFNULL
        assert "IFNULL" not in result.upper()

    def test_postgres_to_drill(self):
        result = transpile_sql(
            "SELECT a::INT FROM t",
            "postgres", "drill"
        )
        assert "CAST" in result.upper()

    def test_with_schemas(self):
        schemas = [{
            "name": "dfs",
            "tables": [{"name": "users", "columns": ["id", "name"]}]
        }]
        result = transpile_sql(
            "SELECT id FROM users",
            "mysql", "drill",
            schemas=schemas
        )
        assert "dfs" in result.lower()

    def test_error_returns_original(self):
        bad = "NOT VALID SQL %%% @@"
        result = transpile_sql(bad, "mysql", "drill")
        assert result == bad


# ===========================================================================
# convert_data_type / convert_data_type_raw
# ===========================================================================

class TestConvertDataType:
    def test_simple_cast(self):
        result = convert_data_type(
            "SELECT price FROM products",
            "price", "INTEGER"
        )
        assert "CAST" in result.upper()
        assert "INTEGER" in result.upper()
        assert "price" in result.lower()

    def test_preserves_existing_alias(self):
        result = convert_data_type(
            "SELECT price AS cost FROM products",
            "price", "DOUBLE"
        )
        assert "CAST" in result.upper()
        assert "cost" in result.lower()

    def test_column_inside_function(self):
        result = convert_data_type(
            "SELECT UPPER(name) AS uname FROM t",
            "name", "VARCHAR"
        )
        assert "CAST" in result.upper()
        assert "VARCHAR" in result.upper()

    def test_star_query_expansion(self):
        result = convert_data_type(
            "SELECT * FROM t",
            "price", "INTEGER",
            column_list={"id": "INT", "price": "VARCHAR", "name": "VARCHAR"}
        )
        assert "CAST" in result.upper()
        assert "*" not in result

    def test_raw_json_input(self):
        columns_json = json.dumps({"id": "INT", "price": "VARCHAR"})
        result = convert_data_type_raw(
            "SELECT * FROM t",
            "price", "INTEGER",
            columns_json
        )
        assert "CAST" in result.upper()

    def test_raw_no_columns(self):
        result = convert_data_type_raw(
            "SELECT price FROM t",
            "price", "INTEGER"
        )
        assert "CAST" in result.upper()


# ===========================================================================
# change_time_grain / change_time_grain_raw
# ===========================================================================

class TestChangeTimeGrain:
    def test_simple_column(self):
        result = change_time_grain(
            "SELECT date, amount FROM t",
            "date", "MONTH", []
        )
        sql = result["sql"]
        assert "date_trunc" in sql.lower()
        assert "MONTH" in sql.upper()
        assert "date" in sql.lower()

    def test_aliased_column_preserved(self):
        result = change_time_grain(
            "SELECT date AS d, amount FROM t",
            "date", "YEAR", []
        )
        sql = result["sql"]
        assert "date_trunc" in sql.lower()
        assert "YEAR" in sql.upper()

    def test_column_inside_function(self):
        result = change_time_grain(
            "SELECT CAST(date AS DATE) AS date, amount FROM t",
            "date", "MONTH", []
        )
        sql = result["sql"]
        assert "date_trunc" in sql.lower()
        assert "MONTH" in sql.upper()

    @pytest.mark.parametrize("grain", ["MONTH", "YEAR", "DAY", "QUARTER"])
    def test_multiple_time_grains(self, grain):
        result = change_time_grain(
            "SELECT date, amount FROM t",
            "date", grain, []
        )
        sql = result["sql"]
        assert grain in sql.upper()
        assert "date_trunc" in sql.lower()

    def test_star_query_expansion(self):
        columns = {"date": "DATE", "amount": "DOUBLE"}
        result = change_time_grain(
            "SELECT * FROM t",
            "date", "MONTH", columns
        )
        sql = result["sql"]
        assert "date_trunc" in sql.lower()
        assert "*" not in sql

    def test_raw_json_input(self):
        columns_json = json.dumps(["date", "amount"])
        result = change_time_grain_raw(
            "SELECT * FROM t",
            "date", "MONTH",
            columns_json
        )
        assert "date_trunc" in result.lower()
        assert "MONTH" in result.upper()

    def test_raw_no_columns(self):
        result = change_time_grain_raw(
            "SELECT date, amount FROM t",
            "date", "YEAR"
        )
        assert "date_trunc" in result.lower()
        assert "YEAR" in result.upper()

    def test_lowercase_grain_normalised(self):
        """Time grain should be uppercased internally."""
        result = change_time_grain(
            "SELECT date, amount FROM t",
            "date", "month", []
        )
        sql = result["sql"]
        assert "MONTH" in sql.upper()
        assert "date_trunc" in sql.lower()

    def test_returns_dict_with_sql_key(self):
        result = change_time_grain(
            "SELECT date FROM t",
            "date", "DAY", []
        )
        assert isinstance(result, dict)
        assert "sql" in result
