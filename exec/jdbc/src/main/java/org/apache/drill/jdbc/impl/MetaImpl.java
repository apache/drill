/**
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
package org.apache.drill.jdbc.impl;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.Types;
import java.util.List;

import net.hydromatic.avatica.AvaticaPrepareResult;
import net.hydromatic.avatica.AvaticaResultSet;
import net.hydromatic.avatica.AvaticaStatement;
import net.hydromatic.avatica.Cursor;
import net.hydromatic.avatica.Meta;

import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.util.DrillStringUtils;


class MetaImpl implements Meta {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MetaImpl.class);

  // TODO:  Use more central version of these constants if available.

  /** JDBC conventional(?) number of fractional decimal digits for REAL. */
  private static final int DECIMAL_DIGITS_REAL = 7;
  /** JDBC conventional(?) number of fractional decimal digits for FLOAT. */
  private static final int DECIMAL_DIGITS_FLOAT = DECIMAL_DIGITS_REAL;
  /** JDBC conventional(?) number of fractional decimal digits for DOUBLE. */
  private static final int DECIMAL_DIGITS_DOUBLE = 15;

  /** Radix used to report precisions of "datetime" types. */
  private static final int RADIX_DATETIME = 10;
  /** Radix used to report precisions of interval types. */
  private static final int RADIX_INTERVAL = 10;


  final DrillConnectionImpl connection;

  MetaImpl(DrillConnectionImpl connection) {
    this.connection = connection;
  }

  @Override
  public String getSqlKeywords() {
    return "";
  }

  @Override
  public String getNumericFunctions() {
    return "";
  }

  @Override
  public String getStringFunctions() {
    return "";
  }

  @Override
  public String getSystemFunctions() {
    return "";
  }

  @Override
  public String getTimeDateFunctions() {
    return "";
  }

  private ResultSet s(String s) {
    try {
      logger.debug("Running {}", s);
      AvaticaStatement statement = connection.createStatement();
      statement.execute(s);
      return statement.getResultSet();

    } catch (Exception e) {
      // Wrap in RuntimeException because Avatica's abstract method declarations
      // didn't allow for SQLException!
      throw new DrillRuntimeException("Failure while attempting to get DatabaseMetadata.", e);
    }

  }

  /**
   * Returns interim generic empty result set.
   * <p>
   *   (Does not return specific columns expected (and visible in metadata) for
   *   specific get methods.)
   * </p>
   */
  private ResultSet getEmptyResultSet() {
    return s(
        "SELECT '' AS `Interim zero-row result set` "  // dummy row type
        + "FROM INFORMATION_SCHEMA.CATALOGS "          // any table
        + "LIMIT 0"                                    // zero rows
        );
  }

  @Override
  public ResultSet getTables(String catalog, final Pat schemaPattern, final Pat tableNamePattern,
      final List<String> typeList) {
    StringBuilder sb = new StringBuilder();
    sb.append("select "
        + "TABLE_CATALOG as TABLE_CAT, "
        + "TABLE_SCHEMA as TABLE_SCHEM, "
        + "TABLE_NAME, "
        + "TABLE_TYPE, "
        + "'' as REMARKS, "
        + "'' as TYPE_CAT, "
        + "'' as TYPE_SCHEM, "
        + "'' as TYPE_NAME, "
        + "'' as SELF_REFERENCING_COL_NAME, "
        + "'' as REF_GENERATION "
        + "FROM INFORMATION_SCHEMA.`TABLES` WHERE 1=1 ");

    if (catalog != null) {
      sb.append(" AND TABLE_CATALOG = '" + DrillStringUtils.escapeSql(catalog) + "' ");
    }

    if (schemaPattern.s != null) {
      sb.append(" AND TABLE_SCHEMA like '" + DrillStringUtils.escapeSql(schemaPattern.s) + "'");
    }

    if (tableNamePattern.s != null) {
      sb.append(" AND TABLE_NAME like '" + DrillStringUtils.escapeSql(tableNamePattern.s) + "'");
    }

    if (typeList != null && typeList.size() > 0) {
      sb.append("AND (");
      for (int t = 0; t < typeList.size(); t++) {
        if (t != 0) {
          sb.append(" OR ");
        }
        sb.append(" TABLE_TYPE LIKE '" + DrillStringUtils.escapeSql(typeList.get(t)) + "' ");
      }
      sb.append(")");
    }

    sb.append(" ORDER BY TABLE_TYPE, TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME");

    return s(sb.toString());
  }

  /**
   * Implements {@link DatabaseMetaData#getColumns}.
   */
  @Override
  public ResultSet getColumns(String catalog, Pat schemaPattern,
                              Pat tableNamePattern, Pat columnNamePattern) {
    StringBuilder sb = new StringBuilder();
    // TODO:  Resolve the various questions noted below.
    sb.append(
        "SELECT "
        // getColumns   INFORMATION_SCHEMA.COLUMNS        getColumns()
        // column       source column or                  column name
        // number       expression
        // -------      ------------------------          -------------
        + /*  1 */ "\n  TABLE_CATALOG                 as  TABLE_CAT, "
        + /*  2 */ "\n  TABLE_SCHEMA                  as  TABLE_SCHEM, "
        + /*  3 */ "\n  TABLE_NAME                    as  TABLE_NAME, "
        + /*  4 */ "\n  COLUMN_NAME                   as  COLUMN_NAME, "

        /*    5                                           DATA_TYPE */
        // TODO:  Resolve the various questions noted below for DATA_TYPE.
        + "\n  CASE DATA_TYPE "
        // (All values in JDBC 4.0/Java 7 java.sql.Types except for types.NULL:)

        + "\n    WHEN 'ARRAY'                       THEN " + Types.ARRAY

        + "\n    WHEN 'BIGINT'                      THEN " + Types.BIGINT
        + "\n    WHEN 'BINARY'                      THEN " + Types.BINARY
        // Resolve:  Not seen in Drill yet.  Can it appear?:
        + "\n    WHEN 'BINARY LARGE OBJECT'         THEN " + Types.BLOB
        + "\n    WHEN 'BINARY VARYING'              THEN " + Types.VARBINARY
        // Resolve:  Not seen in Drill yet.  Can it appear?:
        + "\n    WHEN 'BIT'                         THEN " + Types.BIT
        + "\n    WHEN 'BOOLEAN'                     THEN " + Types.BOOLEAN

        + "\n    WHEN 'CHARACTER'                   THEN " + Types.CHAR
        // Resolve:  Not seen in Drill yet.  Can it appear?:
        + "\n    WHEN 'CHARACTER LARGE OBJECT'      THEN " + Types.CLOB
        + "\n    WHEN 'CHARACTER VARYING'           THEN " + Types.VARCHAR

        // Resolve:  Not seen in Drill yet.  Can it appear?:
        + "\n    WHEN 'DATALINK'                    THEN " + Types.DATALINK
        + "\n    WHEN 'DATE'                        THEN " + Types.DATE
        + "\n    WHEN 'DECIMAL'                     THEN " + Types.DECIMAL
        // Resolve:  Not seen in Drill yet.  Can it appear?:
        + "\n    WHEN 'DISTINCT'                    THEN " + Types.DISTINCT
        + "\n    WHEN 'DOUBLE', 'DOUBLE PRECISION'  THEN " + Types.DOUBLE

        + "\n    WHEN 'FLOAT'                       THEN " + Types.FLOAT

        + "\n    WHEN 'INTEGER'                     THEN " + Types.INTEGER
        + "\n    WHEN 'INTERVAL'                    THEN " + Types.OTHER

        // Resolve:  Not seen in Drill yet.  Can it ever appear?:
        + "\n    WHEN 'JAVA_OBJECT'                 THEN " + Types.JAVA_OBJECT

        // Resolve:  Not seen in Drill yet.  Can it appear?:
        + "\n    WHEN 'LONGNVARCHAR'                THEN " + Types.LONGNVARCHAR
        // Resolve:  Not seen in Drill yet.  Can it appear?:
        + "\n    WHEN 'LONGVARBINARY'               THEN " + Types.LONGVARBINARY
        // Resolve:  Not seen in Drill yet.  Can it appear?:
        + "\n    WHEN 'LONGVARCHAR'                 THEN " + Types.LONGVARCHAR

        + "\n    WHEN 'MAP'                         THEN " + Types.OTHER

        // Resolve:  Not seen in Drill yet.  Can it appear?:
        + "\n    WHEN 'NATIONAL CHARACTER'          THEN " + Types.NCHAR
        // Resolve:  Not seen in Drill yet.  Can it appear?:
        + "\n    WHEN 'NATIONAL CHARACTER LARGE OBJECT' "
        + "\n                                       THEN " + Types.NCLOB
        // TODO:  Resolve following about NULL (and then update comment and code):
        // It is not clear whether Types.NULL can represent a type (perhaps the
        // type of the literal NULL when no further type information is known?) or
        // whether 'NULL' can appear in INFORMATION_SCHEMA.COLUMNS.DATA_TYPE.
        // For now, since it shouldn't hurt, include 'NULL'/Types.NULL in mapping.
        + "\n    WHEN 'NULL'                        THEN " + Types.NULL
        // (No NUMERIC--Drill seems to map any to DECIMAL currently.)
        + "\n    WHEN 'NUMERIC'                     THEN " + Types.NUMERIC
        // Resolve:  Not seen in Drill yet.  Can it appear?:
        + "\n    WHEN 'NATIONAL CHARACTER'          THEN " + Types.NCHAR
        // Resolve:  Not seen in Drill yet.  Can it appear?:
        + "\n    WHEN 'NATIONAL CHARACTER VARYING'  THEN " + Types.NVARCHAR

        // Resolve:  Unexpectedly, has appeared in Drill.  Should it?
        + "\n    WHEN 'OTHER'                       THEN " + Types.OTHER

        + "\n    WHEN 'REAL'                        THEN " + Types.REAL
        // Resolve:  Not seen in Drill yet.  Can it appear?:
        + "\n    WHEN 'REF'                         THEN " + Types.REF
        // Resolve:  Not seen in Drill yet.  Can it appear?:
        + "\n    WHEN 'ROWID'                       THEN " + Types.ROWID

        + "\n    WHEN 'SMALLINT'                    THEN " + Types.SMALLINT
        // Resolve:  Not seen in Drill yet.  Can it appear?:
        + "\n    WHEN 'SQLXML'                      THEN " + Types.SQLXML
        + "\n    WHEN 'STRUCT'                      THEN " + Types.STRUCT

        + "\n    WHEN 'TIME'                        THEN " + Types.TIME
        + "\n    WHEN 'TIMESTAMP'                   THEN " + Types.TIMESTAMP
        + "\n    WHEN 'TINYINT'                     THEN " + Types.TINYINT

        + "\n    ELSE                                    " + Types.OTHER
        + "\n  END                                    as  DATA_TYPE, "

        + /*  6 */ "\n  DATA_TYPE                     as  TYPE_NAME, "

        /*    7                                           COLUMN_SIZE */
        /* "... COLUMN_SIZE ....
         * For numeric data, this is the maximum precision.
         * For character data, this is the length in characters.
         * For datetime datatypes, this is the length in characters of the String
         *   representation (assuming the maximum allowed precision of the
         *   fractional seconds component).
         * For binary data, this is the length in bytes.
         * For the ROWID datatype, this is the length in bytes.
         * Null is returned for data types where the column size is not applicable."
         *
         * Note:  "Maximum precision" seems to mean the maximum number of
         * significant digits that can appear (not the number of decimal digits
         * that can be counted on, and not the maximum number of (decimal)
         * characters needed to display a value).
         */
        + "\n  CASE DATA_TYPE "

        // 1. "For numeric data, ... the maximum precision":
        + "\n    WHEN 'TINYINT', 'SMALLINT', 'INTEGER', 'BIGINT', "
        + "\n         'DECIMAL', 'NUMERIC', "
        + "\n         'REAL', 'FLOAT', 'DOUBLE' "
        + "\n                         THEN NUMERIC_PRECISION "

        // 2. "For character data, ... the length in characters":
        + "\n    WHEN 'CHARACTER', 'CHARACTER VARYING' "
        + "\n                         THEN CHARACTER_MAXIMUM_LENGTH "

        // 3. "For datetime datatypes ... length ... String representation
        //    (assuming the maximum ... precision of ... fractional seconds ...)":
        // SQL datetime types:
        + "\n    WHEN 'DATE'          THEN 10 "            // YYYY-MM-DD
        + "\n    WHEN 'TIME'          THEN "
        + "\n      CASE "
        + "\n        WHEN DATETIME_PRECISION > 0 "         // HH:MM:SS.sss
        + "\n                         THEN          8 + 1 + DATETIME_PRECISION"
        + "\n        ELSE                           8"     // HH:MM:SS
        + "\n      END "
        + "\n    WHEN 'TIMESTAMP'     THEN "
        + "\n      CASE "                                  // date + "T" + time
        + "\n        WHEN DATETIME_PRECISION > 0 "
        + "                           THEN 10 + 1 + 8 + 1 + DATETIME_PRECISION"
        + "\n        ELSE                  10 + 1 + 8"
        + "\n      END "
        // SQL interval types:
        // Note:  Not addressed by JDBC 4.1; providing length of current string
        // representation (not length of, say, interval literal).
        + "\n    WHEN 'INTERVAL'      THEN "
        + "\n      INTERVAL_PRECISION "
        + "\n      + "
        + "\n      CASE INTERVAL_TYPE "
        // a. Single field, not SECOND:
        + "\n        WHEN 'YEAR', 'MONTH', 'DAY' THEN 2 "  // like P...Y
        + "\n        WHEN 'HOUR', 'MINUTE'       THEN 3 "  // like PT...M
        // b. Two adjacent fields, no SECOND:
        + "\n        WHEN 'YEAR TO MONTH'        THEN 5 "  // P...Y12M
        + "\n        WHEN 'DAY TO HOUR'          THEN 6 "  // P...DT12H
        + "\n        WHEN 'HOUR TO MINUTE'       THEN 6 "  // PT...H12M
        // c. Three contiguous fields, no SECOND:
        + "\n        WHEN 'DAY TO MINUTE'        THEN 9 "  // P...DT12H12M
        // d. With SECOND field:
        + "\n        ELSE "
        + "\n          CASE INTERVAL_TYPE "
        + "\n            WHEN 'DAY TO SECOND'    THEN 12 " // P...DT12H12M12...S
        + "\n            WHEN 'HOUR TO SECOND'   THEN  9 " // PT...H12M12...S
        + "\n            WHEN 'MINUTE TO SECOND' THEN  6 " // PT...M12...S
        + "\n            WHEN 'SECOND'           THEN  3 " // PT......S
        + "\n            ELSE "                  // Make net result be -1:
        // WORKAROUND:  This "0" is to work around Drill's failure to support
        // unary minus syntax (negation):
        + "\n                                    0-INTERVAL_PRECISION - 1 "
        + "\n          END "
        + "\n          + "
        + "\n          DATETIME_PRECISION"
        + "\n          + "
        + "\n          CASE " // If frac. digits, also add 1 for decimal point.
        + "\n            WHEN DATETIME_PRECISION > 0 THEN 1"
        + "\n            ELSE                             0 "
        + "\n          END"
        // - For INTERVAL ... TO SECOND(0): "P...DT12H12M12S"
        + "\n      END "

        // 4. "For binary data, ... the length in bytes":
        + "\n    WHEN 'BINARY', 'BINARY VARYING' "
        + "\n                         THEN CHARACTER_MAXIMUM_LENGTH "

        // 5. "For ... ROWID datatype...": Not in Drill?

        // 6. "Null ... for data types [for which] ... not applicable.":
        + "\n    ELSE                      NULL "
        + "\n  END                                    as  COLUMN_SIZE, "

        + /*  8 */ "\n  CHARACTER_MAXIMUM_LENGTH      as  BUFFER_LENGTH, "

        /*    9                                           DECIMAL_DIGITS */
        + "\n  CASE  DATA_TYPE"
        + "\n    WHEN 'TINYINT', 'SMALLINT', 'INTEGER', 'BIGINT', "
        + "\n         'DECIMAL', 'NUMERIC'        THEN NUMERIC_SCALE "
        + "\n    WHEN 'REAL'                      THEN " + DECIMAL_DIGITS_REAL
        + "\n    WHEN 'FLOAT'                     THEN " + DECIMAL_DIGITS_FLOAT
        + "\n    WHEN 'DOUBLE'                    THEN " + DECIMAL_DIGITS_DOUBLE
        + "\n    WHEN 'DATE', 'TIME', 'TIMESTAMP' THEN DATETIME_PRECISION "
        + "\n    WHEN 'INTERVAL'                  THEN DATETIME_PRECISION "
        + "\n  END                                    as  DECIMAL_DIGITS, "

        /*   10                                           NUM_PREC_RADIX */
        + "\n  CASE DATA_TYPE "
        + "\n    WHEN 'TINYINT', 'SMALLINT', 'INTEGER', 'BIGINT', "
        + "\n         'DECIMAL', 'NUMERIC', "
        + "\n         'REAL', 'FLOAT', 'DOUBLE'   THEN NUMERIC_PRECISION_RADIX "
        // (NUMERIC_PRECISION_RADIX is NULL for these:)
        + "\n    WHEN 'INTERVAL'                  THEN " + RADIX_INTERVAL
        + "\n    WHEN 'DATE', 'TIME', 'TIMESTAMP' THEN " + RADIX_DATETIME
        + "\n    ELSE                                  NULL"
        + "\n  END                                    as  NUM_PREC_RADIX, "

        /*   11                                           NULLABLE */
        + "\n  CASE IS_NULLABLE "
        + "\n    WHEN 'YES'      THEN " + DatabaseMetaData.columnNullable
        + "\n    WHEN 'NO'       THEN " + DatabaseMetaData.columnNoNulls
        + "\n    WHEN ''         THEN " + DatabaseMetaData.columnNullableUnknown
        + "\n    ELSE                 -1"
        + "\n  END                                    as  NULLABLE, "

        + /* 12 */ "\n  CAST( NULL as VARCHAR )       as  REMARKS, "
        + /* 13 */ "\n  COLUMN_DEFAULT                as  COLUMN_DEF, "
        + /* 14 */ "\n  0                             as  SQL_DATA_TYPE, "
        + /* 15 */ "\n  0                             as  SQL_DATETIME_SUB, "

        /*   16                                           CHAR_OCTET_LENGTH */
        + "\n  CASE DATA_TYPE"
        + "\n    WHEN 'CHARACTER', "
        + "\n         'CHARACTER VARYING', "
        + "\n         'NATIONAL CHARACTER', "
        + "\n         'NATIONAL CHARACTER VARYING' "
        + "\n                                 THEN CHARACTER_OCTET_LENGTH "
        + "\n    ELSE                              NULL "
        + "\n  END                                    as  CHAR_OCTET_LENGTH, "

        + /* 17 */ "\n  ORDINAL_POSITION              as  ORDINAL_POSITION, "
        + /* 18 */ "\n  IS_NULLABLE                   as  IS_NULLABLE, "
        + /* 19 */ "\n  CAST( NULL as VARCHAR )       as  SCOPE_CATALOG, "
        + /* 20 */ "\n  CAST( NULL as VARCHAR )       as  SCOPE_SCHEMA, "
        + /* 21 */ "\n  CAST( NULL as VARCHAR )       as  SCOPE_TABLE, "
        // TODO:  Change to SMALLINT when it's implemented (DRILL-2470):
        + /* 22 */ "\n  CAST( NULL as INTEGER )       as  SOURCE_DATA_TYPE, "
        + /* 23 */ "\n  ''                            as  IS_AUTOINCREMENT, "
        + /* 24 */ "\n  ''                            as  IS_GENERATEDCOLUMN "

        + "\n  FROM INFORMATION_SCHEMA.COLUMNS "
        + "\n  WHERE 1=1 ");

    if (catalog != null) {
      sb.append("\n  AND TABLE_CATALOG = '" + DrillStringUtils.escapeSql(catalog) + "'");
    }
    if (schemaPattern.s != null) {
      sb.append("\n  AND TABLE_SCHEMA like '" + DrillStringUtils.escapeSql(schemaPattern.s) + "'");
    }
    if (tableNamePattern.s != null) {
      sb.append("\n  AND TABLE_NAME like '" + DrillStringUtils.escapeSql(tableNamePattern.s) + "'");
    }
    if (columnNamePattern.s != null) {
      sb.append("\n  AND COLUMN_NAME like '" + DrillStringUtils.escapeSql(columnNamePattern.s) + "'");
    }

    sb.append("\n ORDER BY TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME");

    return s(sb.toString());
  }

  @Override
  public ResultSet getSchemas(String catalog, Pat schemaPattern) {
    StringBuilder sb = new StringBuilder();
    sb.append("select "
        + "SCHEMA_NAME as TABLE_SCHEM, "
        + "CATALOG_NAME as TABLE_CAT "
        + " FROM INFORMATION_SCHEMA.SCHEMATA WHERE 1=1 ");

    if (catalog != null) {
      sb.append(" AND CATALOG_NAME = '" + DrillStringUtils.escapeSql(catalog) + "' ");
    }
    if (schemaPattern.s != null) {
      sb.append(" AND SCHEMA_NAME like '" + DrillStringUtils.escapeSql(schemaPattern.s) + "'");
    }
    sb.append(" ORDER BY CATALOG_NAME, SCHEMA_NAME");

    return s(sb.toString());
  }

  @Override
  public ResultSet getCatalogs() {
    StringBuilder sb = new StringBuilder();
    sb.append("select "
        + "CATALOG_NAME as TABLE_CAT "
        + " FROM INFORMATION_SCHEMA.CATALOGS ");

    sb.append(" ORDER BY CATALOG_NAME");

    return s(sb.toString());
  }

  @Override
  public ResultSet getTableTypes() {
    return getEmptyResultSet();
  }

  @Override
  public ResultSet getProcedures(String catalog, Pat schemaPattern, Pat procedureNamePattern) {
    return getEmptyResultSet();
  }

  @Override
  public ResultSet getProcedureColumns(String catalog, Pat schemaPattern, Pat procedureNamePattern,
      Pat columnNamePattern) {
    return getEmptyResultSet();
  }

  @Override
  public ResultSet getColumnPrivileges(String catalog, String schema, String table, Pat columnNamePattern) {
    return getEmptyResultSet();
  }

  @Override
  public ResultSet getTablePrivileges(String catalog, Pat schemaPattern, Pat tableNamePattern) {
    return getEmptyResultSet();
  }

  @Override
  public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable) {
    return getEmptyResultSet();
  }

  @Override
  public ResultSet getVersionColumns(String catalog, String schema, String table) {
    return getEmptyResultSet();
  }

  @Override
  public ResultSet getPrimaryKeys(String catalog, String schema, String table) {
    return getEmptyResultSet();
  }

  @Override
  public ResultSet getImportedKeys(String catalog, String schema, String table) {
    return getEmptyResultSet();
  }

  @Override
  public ResultSet getExportedKeys(String catalog, String schema, String table) {
    return getEmptyResultSet();
  }

  @Override
  public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable,
      String foreignCatalog, String foreignSchema, String foreignTable) {
    return getEmptyResultSet();
  }

  @Override
  public ResultSet getTypeInfo() {
    return getEmptyResultSet();
  }

  @Override
  public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate) {
    return getEmptyResultSet();
  }

  @Override
  public ResultSet getUDTs(String catalog, Pat schemaPattern, Pat typeNamePattern, int[] types) {
    return getEmptyResultSet();
  }

  @Override
  public ResultSet getSuperTypes(String catalog, Pat schemaPattern, Pat typeNamePattern) {
    return getEmptyResultSet();
  }

  @Override
  public ResultSet getSuperTables(String catalog, Pat schemaPattern, Pat tableNamePattern) {
    return getEmptyResultSet();
  }

  @Override
  public ResultSet getAttributes(String catalog, Pat schemaPattern, Pat typeNamePattern, Pat attributeNamePattern) {
    return getEmptyResultSet();
  }

  @Override
  public ResultSet getClientInfoProperties() {
    return getEmptyResultSet();
  }

  @Override
  public ResultSet getFunctions(String catalog, Pat schemaPattern, Pat functionNamePattern) {
    return getEmptyResultSet();
  }

  @Override
  public ResultSet getFunctionColumns(String catalog, Pat schemaPattern, Pat functionNamePattern, Pat columnNamePattern) {
    return getEmptyResultSet();
  }

  @Override
  public ResultSet getPseudoColumns(String catalog, Pat schemaPattern, Pat tableNamePattern, Pat columnNamePattern) {
    return getEmptyResultSet();
  }

  @Override
  public Cursor createCursor(AvaticaResultSet resultSet_) {
    return ((DrillResultSetImpl) resultSet_).cursor;
  }

  @Override
  public AvaticaPrepareResult prepare(AvaticaStatement statement_, String sql) {
    //DrillStatement statement = (DrillStatement) statement_;
    return new DrillPrepareResult(sql);
  }

  interface Named {
    String getName();
  }

}
