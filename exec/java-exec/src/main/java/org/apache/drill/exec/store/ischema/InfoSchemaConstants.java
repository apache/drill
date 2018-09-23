/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store.ischema;

public interface InfoSchemaConstants {

  /** Name of catalog containing information schema. */
  String IS_CATALOG_NAME = "DRILL";

  /** Catalog description */
  String IS_CATALOG_DESCRIPTION = "The internal metadata used by Drill";

  /** Catalog connect string. Currently empty */
   String IS_CATALOG_CONNECT = "";

  /** Name of information schema. */
   String IS_SCHEMA_NAME = "information_schema";

  // CATALOGS column names:
   String CATS_COL_CATALOG_CONNECT = "CATALOG_CONNECT";
   String CATS_COL_CATALOG_DESCRIPTION = "CATALOG_DESCRIPTION";
   String CATS_COL_CATALOG_NAME = "CATALOG_NAME";

  // SCHEMATA column names:
   String SCHS_COL_CATALOG_NAME = "CATALOG_NAME";
   String SCHS_COL_SCHEMA_NAME = "SCHEMA_NAME";
   String SCHS_COL_SCHEMA_OWNER = "SCHEMA_OWNER";
   String SCHS_COL_TYPE = "TYPE";
   String SCHS_COL_IS_MUTABLE = "IS_MUTABLE";

  // Common TABLES / VIEWS / COLUMNS columns names:
   String SHRD_COL_TABLE_CATALOG = "TABLE_CATALOG";
   String SHRD_COL_TABLE_SCHEMA = "TABLE_SCHEMA";
   String SHRD_COL_TABLE_NAME = "TABLE_NAME";

  // Remaining TABLES column names:
   String TBLS_COL_TABLE_TYPE = "TABLE_TYPE";

  // Remaining VIEWS column names:
   String VIEWS_COL_VIEW_DEFINITION = "VIEW_DEFINITION";

  // COLUMNS columns, from SQL standard:
  // 1. TABLE_CATALOG
  // 2. TABLE_SCHEMA
  // 3. TABLE_NAME
  // 4. COLUMN_NAME
  // 5. ORDINAL_POSITION
  // 6. COLUMN_DEFAULT
  // 7. IS_NULLABLE
  // 8. DATA_TYPE
  // 9. CHARACTER_MAXIMUM_LENGTH
  // 10. CHARACTER_OCTET_LENGTH
  // 11. NUMERIC_PRECISION
  // 12. NUMERIC_PRECISION_RADIX
  // 13. NUMERIC_SCALE
  // 14. DATETIME_PRECISION
  // 15. INTERVAL_TYPE
  // 16. INTERVAL_PRECISION
  // 17. CHARACTER_SET_CATALOG ...

  // Remaining COLUMNS column names:
   String COLS_COL_COLUMN_NAME = "COLUMN_NAME";
   String COLS_COL_ORDINAL_POSITION = "ORDINAL_POSITION";
   String COLS_COL_COLUMN_DEFAULT = "COLUMN_DEFAULT";
   String COLS_COL_IS_NULLABLE = "IS_NULLABLE";
   String COLS_COL_DATA_TYPE = "DATA_TYPE";
   String COLS_COL_CHARACTER_MAXIMUM_LENGTH = "CHARACTER_MAXIMUM_LENGTH";
   String COLS_COL_CHARACTER_OCTET_LENGTH = "CHARACTER_OCTET_LENGTH";
   String COLS_COL_NUMERIC_PRECISION = "NUMERIC_PRECISION";
   String COLS_COL_NUMERIC_PRECISION_RADIX = "NUMERIC_PRECISION_RADIX";
   String COLS_COL_NUMERIC_SCALE = "NUMERIC_SCALE";
   String COLS_COL_DATETIME_PRECISION = "DATETIME_PRECISION";
   String COLS_COL_INTERVAL_TYPE = "INTERVAL_TYPE";
   String COLS_COL_INTERVAL_PRECISION = "INTERVAL_PRECISION";

  // FILES column names:
   String FILES_COL_SCHEMA_NAME = SCHS_COL_SCHEMA_NAME;
   String FILES_COL_ROOT_SCHEMA_NAME = "ROOT_SCHEMA_NAME";
   String FILES_COL_WORKSPACE_NAME = "WORKSPACE_NAME";
   String FILES_COL_FILE_NAME = "FILE_NAME";
   String FILES_COL_RELATIVE_PATH = "RELATIVE_PATH";
   String FILES_COL_IS_DIRECTORY = "IS_DIRECTORY";
   String FILES_COL_IS_FILE = "IS_FILE";
   String FILES_COL_LENGTH = "LENGTH";
   String FILES_COL_OWNER = "OWNER";
   String FILES_COL_GROUP = "GROUP";
   String FILES_COL_PERMISSION = "PERMISSION";
   String FILES_COL_ACCESS_TIME = "ACCESS_TIME";
   String FILES_COL_MODIFICATION_TIME = "MODIFICATION_TIME";
}
