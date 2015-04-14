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
package org.apache.drill.jdbc;

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
import org.apache.drill.jdbc.impl.DrillResultSetImpl;


public class MetaImpl implements Meta {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MetaImpl.class);

  // TODO:  Use more central version of these constants if availabe.

  /** Radix used to report precision and scale of integral exact numeric types. */
  private static final int RADIX_INTEGRAL = 10;
  /** Radix used to report precision and scale of non-integral exact numeric
      types (DECIMAL). */
  private static final int RADIX_DECIMAL = 10;
  /** Radix used to report precision and scale of approximate numeric types
      (FLOAT, etc.). */
  private static final int RADIX_APPROXIMATE = 10;
  /** Radix used to report precisions of interval types. */
  private static final int RADIX_INTERVAL = 10;

  /** (Maximum) precision of TINYINT. */
  private static final int PREC_TINYINT  = 3;
  /** (Maximum) precision of SMALLINT. */
  private static final int PREC_SMALLINT = 5;
  /** (Maximum) precision of INTEGER. */
  private static final int PREC_INTEGER  = 10;
  /** (Maximum) precision of BIGINT. */
  private static final int PREC_BIGINT   = 19;

  /** Precision of FLOAT. */
  private static final int PREC_FLOAT  =  7;
  /** Precision of DOUBLE. */
  private static final int PREC_DOUBLE = 15;
  /** Precision of REAL. */
  private static final int PREC_REAL   = PREC_DOUBLE;

  /** Scale of INTEGER types. */
  private static final int SCALE_INTEGRAL = 0;
  /** JDBC conventional(?) scale value for FLOAT. */
  private static final int SCALE_FLOAT = 7;
  /** JDBC conventional(?) scale value for DOUBLE. */
  private static final int SCALE_DOUBLE = 15;
  /** JDBC conventional(?) scale value for REAL. */
  private static final int SCALE_REAL = SCALE_DOUBLE;

  /** (Apparent) maximum precision for starting unit of INTERVAL type. */
  private static final int PREC_INTERVAL_LEAD_MAX = 10;
  /** (Apparent) maximum fractional seconds precision for INTERVAL type. */
  private static final int PREC_INTERVAL_TRAIL_MAX = 9;


  final DrillConnectionImpl connection;

  public MetaImpl(DrillConnectionImpl connection) {
    this.connection = connection;
  }

  public String getSqlKeywords() {
    return "";
  }

  public String getNumericFunctions() {
    return "";
  }

  public String getStringFunctions() {
    return "";
  }

  public String getSystemFunctions() {
    return "";
  }

  public String getTimeDateFunctions() {
    return "";
  }

  public static ResultSet getEmptyResultSet() {
    return null;
  }

  private ResultSet s(String s) {
    try {
      logger.debug("Running {}", s);
      AvaticaStatement statement = connection.createStatement();
      statement.execute(s);
      return statement.getResultSet();

    } catch (Exception e) {
      throw new DrillRuntimeException("Failure while attempting to get DatabaseMetadata.", e);
    }

  }

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
   * Implements {@link DatabaseMetaData#getColumns()}.
   */
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

        // Exact-match cases:
        + "\n    WHEN 'BIGINT'                      THEN " + Types.BIGINT
        + "\n    WHEN 'BINARY'                      THEN " + Types.BINARY
        // Resolve:  Not seen in Drill yet.  Can it appear?:
        + "\n    WHEN 'BIT'                         THEN " + Types.BIT
        // Resolve:  Not seen in Drill yet.  Can it appear?:
        + "\n    WHEN 'BLOB', 'BINARY LARGE OBJECT' THEN " + Types.BLOB
        + "\n    WHEN 'BOOLEAN'                     THEN " + Types.BOOLEAN

        + "\n    WHEN 'CHAR', 'CHARACTER'           THEN " + Types.CHAR
        // Resolve:  Not seen in Drill yet.  Can it appear?:
        + "\n    WHEN 'CLOB', 'CHARACTER LARGE OBJECT' "
        + "\n                                       THEN " + Types.CLOB

        // Resolve:  Not seen in Drill yet.  Can it appear?:
        + "\n    WHEN 'DATALINK'                    THEN " + Types.DATALINK
        + "\n    WHEN 'DATE'                        THEN " + Types.DATE
        + "\n    WHEN 'DECIMAL'                     THEN " + Types.DECIMAL
        // Resolve:  Not seen in Drill yet.  Can it appear?:
        + "\n    WHEN 'DISTINCT'                    THEN " + Types.DISTINCT
        + "\n    WHEN 'DOUBLE', 'DOUBLE PRECISION'  THEN " + Types.DOUBLE

        + "\n    WHEN 'FLOAT'                       THEN " + Types.FLOAT

        + "\n    WHEN 'INTEGER'                     THEN " + Types.INTEGER

        // Drill's INFORMATION_SCHEMA's COLUMNS currently has
        // "INTERVAL_YEAR_MONTH" and "INTERVAL_DAY_TIME" instead of SQL standard
        // 'INTERVAL'.
        + "\n    WHEN 'INTERVAL', "
        + "\n         'INTERVAL_YEAR_MONTH', "
        + "\n         'INTERVAL_DAY_TIME'           THEN " + Types.OTHER

        // Resolve:  Not seen in Drill yet.  Can it ever appear?:
        + "\n    WHEN 'JAVA_OBJECT'                 THEN " + Types.JAVA_OBJECT

        // Resolve:  Not seen in Drill yet.  Can it appear?:
        + "\n    WHEN 'LONGNVARCHAR'                THEN " + Types.LONGNVARCHAR
        // Resolve:  Not seen in Drill yet.  Can it appear?:
        + "\n    WHEN 'LONGVARBINARY'               THEN " + Types.LONGVARBINARY
        // Resolve:  Not seen in Drill yet.  Can it appear?:
        + "\n    WHEN 'LONGVARCHAR'                 THEN " + Types.LONGVARCHAR

        // Resolve:  Not seen in Drill yet.  Can it appear?:
        + "\n    WHEN 'NCHAR', 'NATIONAL CHARACTER' THEN " + Types.NCHAR
        // Resolve:  Not seen in Drill yet.  Can it appear?:
        + "\n    WHEN 'NCLOB', 'NATIONAL CHARACTER LARGE OBJECT' "
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
        + "\n    WHEN 'NVARCHAR', 'NATIONAL CHARACTER VARYING' "
        + "\n                                       THEN " + Types.NVARCHAR

        // Resolve:  Unexpectedly, has appeared in Drill.  Should it?
        + "\n    WHEN 'OTHER'                       THEN " + Types.OTHER

        + "\n    WHEN 'REAL'                        THEN " + Types.REAL
        // SQL source syntax:
        //   <reference type> ::=
        //     REF <left paren> <referenced type> <right paren> [ <scope clause> ]
        // Resolve:  Not seen in Drill yet.  Can it appear?:
        + "\n    WHEN 'REF'                         THEN " + Types.REF
        // Resolve:  Not seen in Drill yet.  Can it appear?:
        + "\n    WHEN 'ROWID'                       THEN " + Types.ROWID

        + "\n    WHEN 'SMALLINT'                    THEN " + Types.SMALLINT
        // Resolve:  Not seen in Drill yet.  Can it appear?:
        + "\n    WHEN 'SQLXML'                      THEN " + Types.SQLXML

        + "\n    WHEN 'TIME'                        THEN " + Types.TIME
        + "\n    WHEN 'TIMESTAMP'                   THEN " + Types.TIMESTAMP
        + "\n    WHEN 'TINYINT'                     THEN " + Types.TINYINT

        + "\n    WHEN 'VARBINARY', 'BINARY VARYING' THEN " + Types.VARBINARY
        + "\n    WHEN 'VARCHAR', 'CHARACTER VARYING' "
        + "\n                                       THEN " + Types.VARCHAR

        + "\n    ELSE"
        // Pattern-match cases:
        + "\n      CASE "

        // TODO:  RESOLVE:  How does ARRAY appear in COLUMNS.DATA_TYPE?
        // - Only at end (with no maximum size, as "VARCHAR(65535) ARRAY")?
        // - Possibly with maximum size (as "... ARRAY[10]")?
        // - Then, how should it appear in JDBC ("ARRAY"? "... ARRAY"?)
        // (SQL source syntax:
        //   <array type> ::=
        //     <data type> ARRAY
        //       [ <left bracket or trigraph> <maximum cardinality>
        //         <right bracket or trigraph> ]
        + "\n        WHEN DATA_TYPE LIKE '% ARRAY'  THEN " + Types.ARRAY

        // TODO:  RESOLVE:  How does MAP appear in COLUMNS.DATA_TYPE?
        // - Only at end?
        // - Otherwise?
        // TODO:  RESOLVE:  Should it map to Types.OTHER or something else?
        // Has appeared in Drill.  Should it?
        + "\n        WHEN DATA_TYPE LIKE '% MAP'    THEN " + Types.OTHER

        // TODO:  RESOLVE:  How does "STRUCT" appear?
        // - Only at beginning (as "STRUCT(INTEGER sint, BOOLEAN sboolean")?
        // - Otherwise too?
        // - Then, how should it appear in JDBC ("STRUCT"? "STRUCT(...)"?)
        + "\n        WHEN DATA_TYPE LIKE 'STRUCT(%' THEN " + Types.STRUCT

        + "\n        ELSE                                " + Types.OTHER
        + "\n      END "
        + "\n  END                                    as  DATA_TYPE, "

        /*    6                                           TYPE_NAME */
        // Map Drill's current info. schema values to what SQL standard
        // specifies (for DATA_TYPE)--and assume that that's what JDBC wants.
        + "\n  CASE DATA_TYPE "
        + "\n    WHEN 'INTERVAL_YEAR_MONTH', "
        + "\n         'INTERVAL_DAY_TIME'     THEN 'INTERVAL'"
        // TODO:  Resolve how non-scalar types should appear in
        // INFORMATION_SCHEMA.COLUMNS and here in JDBC:
        // - "ARRAY" or "... ARRAY"?
        // - "MAP" or "... MAP"?
        // - "STRUCT" or "STRUCT(...)"?
        + "\n    ELSE                               DATA_TYPE "
        + "\n  END                                    as TYPE_NAME, "

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
         * significant decimal digits that can appear (not the number of digits
         * that can be counted on, and not the maximum number of characters
         * needed to display a value).
         */
        + "\n  CASE DATA_TYPE "

        // "For numeric data, ... the maximum precision":
        //   TODO:  Change literals to references to declared constant fields:
        // - exact numeric types:
        //   (in decimal digits, coordinated with NUM_PREC_RADIX = 10)
        + "\n    WHEN 'TINYINT'                      THEN " + PREC_TINYINT
        + "\n    WHEN 'SMALLINT'                     THEN " + PREC_SMALLINT
        + "\n    WHEN 'INTEGER'                      THEN " + PREC_INTEGER
        + "\n    WHEN 'BIGINT'                       THEN " + PREC_BIGINT
        + "\n    WHEN 'DECIMAL', 'NUMERIC'           THEN NUMERIC_PRECISION "
        // - approximate numeric types:
        //   (in decimal digits, coordinated with NUM_PREC_RADIX = 10)
        // TODO:  REVISIT:  Should these be in bits or decimal digits (with
        //   NUM_PREC_RADIX coordinated)?  INFORMATION_SCHEMA.COLUMNS's value
        //   are supposed to be in bits (per the SQL spec.).  What does JDBC
        //   require and allow?
        + "\n    WHEN 'FLOAT'                        THEN " + PREC_FLOAT
        + "\n    WHEN 'DOUBLE'                       THEN " + PREC_DOUBLE
        + "\n    WHEN 'REAL'                         THEN " + PREC_REAL

        // "For character data, ... the length in characters":
        // TODO:  BUG:  DRILL-2459:  For CHARACTER / CHAR, length is not in
        // CHARACTER_MAXIMUM_LENGTH but in NUMERIC_PRECISION.
        // Workaround:
        + "\n    WHEN 'VARCHAR', 'CHARACTER VARYING' "
        + "\n                                    THEN CHARACTER_MAXIMUM_LENGTH "
        + "\n    WHEN 'CHAR', 'CHARACTER', "
        + "\n         'NCHAR', 'NATIONAL CHAR', 'NATIONAL CHARACTER' "
        + "\n                                        THEN NUMERIC_PRECISION "

        // "For datetime datatypes ... length ... String representation
        // (assuming the maximum ... precision of ... fractional seconds ...)":
        + "\n    WHEN 'DATE'            THEN 10 "              // YYYY-MM-DD
        + "\n    WHEN 'TIME'            THEN "
        + "\n      CASE "
        + "\n        WHEN NUMERIC_PRECISION > 0 "              // HH:MM:SS.sss
        + "\n                           THEN          8 + 1 + NUMERIC_PRECISION"
        + "\n        ELSE                             8"       // HH:MM:SS
        + "\n      END "
        + "\n    WHEN 'TIMESTAMP'       THEN "
        + "\n      CASE "                          // date + "T" + time (above)
        + "\n        WHEN NUMERIC_PRECISION > 0 "
        + "                             THEN 10 + 1 + 8 + 1 + NUMERIC_PRECISION"
        + "\n        ELSE                    10 + 1 + 8"
        + "\n      END "

        // TODO:  DRILL-2531:  When DRILL-2519 is fixed, use start and end unit
        // and start-unit precision to implement maximum width more precisely
        // (narrowly) than this workaround:
        // For INTERVAL_YEAR_MONTH, maximum width is from "P1234567890Y12M"
        // (5 + apparent maximum start unit precision of 10)
        // unit precision):
        + "\n    WHEN 'INTERVAL_YEAR_MONTH' "
        + "\n                                        THEN 5 + "
                                                          + PREC_INTERVAL_LEAD_MAX
        // For INTERVAL_DAY_TIME, maximum width is from
        // "P1234567890D12H12M12.123456789S" (12 + apparent maximum start unit
        // precision of 10 + apparent maximum seconds fractional precision of 9):
        + "\n    WHEN 'INTERVAL_DAY_TIME' "
        + "\n                                        THEN 12 + "
                                                          + ( PREC_INTERVAL_LEAD_MAX
                                                             + PREC_INTERVAL_TRAIL_MAX )

        // "For binary data, ... the length in bytes":
        // BUG:  DRILL-2459:  BINARY and BINARY VARYING / VARBINARY length is
        // not in CHARACTER_MAXIMUM_LENGTH but in NUMERIC_PRECISION.
        // Workaround:
        + "\n    WHEN 'VARBINARY', 'BINARY VARYING', "
        + "\n         'BINARY'                       THEN NUMERIC_PRECISION "

        // "For ... ROWID datatype...": Not in Drill?

        // "Null ... for data types [for which] ... not applicable.":
        + "\n    ELSE                                     NULL "
        + "\n  END                                    as  COLUMN_SIZE, "

        + /*  8 */ "\n  CHARACTER_MAXIMUM_LENGTH      as  BUFFER_LENGTH, "

        /*    9                                           DECIMAL_DIGITS */
        + "\n  CASE  DATA_TYPE"
        + "\n    WHEN 'TINYINT', "
        + "\n         'SMALLINT', "
        + "\n         'INTEGER', "
        + "\n         'BIGINT'                       THEN " + SCALE_INTEGRAL
        + "\n    WHEN 'DECIMAL', "
        + "\n         'NUMERIC'                      THEN NUMERIC_SCALE "
        + "\n    WHEN 'FLOAT'                        THEN " + SCALE_FLOAT
        + "\n    WHEN 'DOUBLE'                       THEN " + SCALE_DOUBLE
        + "\n    WHEN 'REAL'                         THEN " + SCALE_REAL
        + "\n    WHEN 'INTERVAL'                     THEN NUMERIC_SCALE "
        + "\n    WHEN 'INTERVAL_YEAR_MONTH'          THEN 0 "
        + "\n    WHEN 'INTERVAL_DAY_TIME'            THEN NUMERIC_SCALE "
        + "\n  END                                    as  DECIMAL_DIGITS, "

        /*   10                                           NUM_PREC_RADIX */
        + "\n  CASE DATA_TYPE "
        + "\n    WHEN 'TINYINT', "
        + "\n         'SMALLINT', "
        + "\n         'INTEGER', "
        + "\n         'BIGINT'                       THEN " + RADIX_INTEGRAL
        + "\n    WHEN 'DECIMAL', "
        + "\n         'NUMERIC'                      THEN " + RADIX_DECIMAL
        + "\n    WHEN 'FLOAT', "
        + "\n         'DOUBLE', "
        + "\n         'REAL'                         THEN " + RADIX_APPROXIMATE
        + "\n    WHEN 'INTERVAL_YEAR_MONTH', "
        + "\n         'INTERVAL_DAY_TIME'            THEN " + RADIX_INTERVAL
        + "\n    ELSE                                     NULL"
        + "\n  END                                    as  NUM_PREC_RADIX, "

        /*   11                                           NULLABLE */
        + "\n  CASE IS_NULLABLE "
        + "\n    WHEN 'YES'      THEN " + DatabaseMetaData.columnNullable
        + "\n    WHEN 'NO'       THEN " + DatabaseMetaData.columnNoNulls
        + "\n    WHEN ''         THEN " + DatabaseMetaData.columnNullableUnknown
        + "\n    ELSE                 -1"
        + "\n  END                                    as  NULLABLE, "

        + /* 12 */ "\n  CAST( NULL as VARCHAR )       as  REMARKS, "
        + /* 13 */ "\n  CAST( NULL as VARCHAR )       as  COLUMN_DEF, "
        + /* 14 */ "\n  0                             as  SQL_DATA_TYPE, "
        + /* 15 */ "\n  0                             as  SQL_DATETIME_SUB, "

        /*   16                                           CHAR_OCTET_LENGTH */
        + "\n  CASE DATA_TYPE"
        + "\n    WHEN 'VARCHAR', 'CHARACTER VARYING' "
        + "\n                                 THEN 4 * CHARACTER_MAXIMUM_LENGTH "
        + "\n    WHEN 'CHAR', 'CHARACTER', "
        + "\n         'NCHAR', 'NATIONAL CHAR', 'NATIONAL CHARACTER' "
        // TODO:  BUG:  DRILL-2459:  For CHARACTER / CHAR, length is not in
        // CHARACTER_MAXIMUM_LENGTH but in NUMERIC_PRECISION.  Workaround:
        + "\n                                 THEN 4 * NUMERIC_PRECISION "
        + "\n    ELSE                              NULL "
        + "\n  END                                    as  CHAR_OCTET_LENGTH, "

        + /* 17 */ "\n  1 + ORDINAL_POSITION          as  ORDINAL_POSITION, "
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

  public ResultSet getCatalogs() {
    StringBuilder sb = new StringBuilder();
    sb.append("select "
        + "CATALOG_NAME as TABLE_CAT "
        + " FROM INFORMATION_SCHEMA.CATALOGS ");

    sb.append(" ORDER BY CATALOG_NAME");

    return s(sb.toString());
  }

  public ResultSet getTableTypes() {
    return getEmptyResultSet();
  }

  public ResultSet getProcedures(String catalog, Pat schemaPattern, Pat procedureNamePattern) {
    return getEmptyResultSet();
  }

  public ResultSet getProcedureColumns(String catalog, Pat schemaPattern, Pat procedureNamePattern,
      Pat columnNamePattern) {
    return getEmptyResultSet();
  }

  public ResultSet getColumnPrivileges(String catalog, String schema, String table, Pat columnNamePattern) {
    return getEmptyResultSet();
  }

  public ResultSet getTablePrivileges(String catalog, Pat schemaPattern, Pat tableNamePattern) {
    return getEmptyResultSet();
  }

  public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable) {
    return getEmptyResultSet();
  }

  public ResultSet getVersionColumns(String catalog, String schema, String table) {
    return getEmptyResultSet();
  }

  public ResultSet getPrimaryKeys(String catalog, String schema, String table) {
    return getEmptyResultSet();
  }

  public ResultSet getImportedKeys(String catalog, String schema, String table) {
    return getEmptyResultSet();
  }

  public ResultSet getExportedKeys(String catalog, String schema, String table) {
    return getEmptyResultSet();
  }

  public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable,
      String foreignCatalog, String foreignSchema, String foreignTable) {
    return getEmptyResultSet();
  }

  public ResultSet getTypeInfo() {
    return getEmptyResultSet();
  }

  public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate) {
    return getEmptyResultSet();
  }

  public ResultSet getUDTs(String catalog, Pat schemaPattern, Pat typeNamePattern, int[] types) {
    return getEmptyResultSet();
  }

  public ResultSet getSuperTypes(String catalog, Pat schemaPattern, Pat typeNamePattern) {
    return getEmptyResultSet();
  }

  public ResultSet getSuperTables(String catalog, Pat schemaPattern, Pat tableNamePattern) {
    return getEmptyResultSet();
  }

  public ResultSet getAttributes(String catalog, Pat schemaPattern, Pat typeNamePattern, Pat attributeNamePattern) {
    return getEmptyResultSet();
  }

  public ResultSet getClientInfoProperties() {
    return getEmptyResultSet();
  }

  public ResultSet getFunctions(String catalog, Pat schemaPattern, Pat functionNamePattern) {
    return getEmptyResultSet();
  }

  public ResultSet getFunctionColumns(String catalog, Pat schemaPattern, Pat functionNamePattern, Pat columnNamePattern) {
    return getEmptyResultSet();
  }

  public ResultSet getPseudoColumns(String catalog, Pat schemaPattern, Pat tableNamePattern, Pat columnNamePattern) {
    return getEmptyResultSet();
  }

  public Cursor createCursor(AvaticaResultSet resultSet_) {
    return ((DrillResultSetImpl) resultSet_).cursor;
  }

  public AvaticaPrepareResult prepare(AvaticaStatement statement_, String sql) {
    //DrillStatement statement = (DrillStatement) statement_;
    return new DrillPrepareResult(sql);
  }

  interface Named {
    String getName();
  }

}
