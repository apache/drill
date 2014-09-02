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
import java.util.List;

import net.hydromatic.avatica.AvaticaPrepareResult;
import net.hydromatic.avatica.AvaticaResultSet;
import net.hydromatic.avatica.AvaticaStatement;
import net.hydromatic.avatica.Cursor;
import net.hydromatic.avatica.Meta;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.drill.common.exceptions.DrillRuntimeException;


public class MetaImpl implements Meta {

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MetaImpl.class);

  static final Driver DRIVER = new Driver();

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

  private ResultSet s(String s){
    try{
      logger.debug("Running {}", s);
      AvaticaStatement statement = connection.createStatement();
      statement.execute(s);
      return statement.getResultSet();

    }catch(Exception e){
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

    if(catalog != null){
      sb.append(" AND TABLE_CATALOG = '" + StringEscapeUtils.escapeSql(catalog) + "' ");
    }

    if(schemaPattern.s != null){
      sb.append(" AND TABLE_SCHEMA like '" + StringEscapeUtils.escapeSql(schemaPattern.s) + "'");
    }

    if(tableNamePattern.s != null){
      sb.append(" AND TABLE_NAME like '" + StringEscapeUtils.escapeSql(tableNamePattern.s) + "'");
    }

    if(typeList != null && typeList.size() > 0){
      sb.append("AND (");
      for(int t = 0; t < typeList.size(); t++){
        if(t != 0) sb.append(" OR ");
        sb.append(" TABLE_TYPE LIKE '" + StringEscapeUtils.escapeSql(typeList.get(t)) + "' ");
      }
      sb.append(")");
    }

    sb.append(" ORDER BY TABLE_TYPE, TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME");

    return s(sb.toString());
  }

  public ResultSet getColumns(String catalog, Pat schemaPattern, Pat tableNamePattern, Pat columnNamePattern) {

    StringBuilder sb = new StringBuilder();
    sb.append("select "
        + "TABLE_CATALOG as TABLE_CAT, "
        + "TABLE_SCHEMA as TABLE_SCHEM, "
        + "TABLE_NAME, "
        + "COLUMN_NAME, "
        + "DATA_TYPE, "
        + "CHARACTER_MAXIMUM_LENGTH as BUFFER_LENGTH, "
        + "NUMERIC_PRECISION as DECIMAL_PRECISION, "
        + "NUMERIC_PRECISION_RADIX as NUM_PREC_RADIX, "
        + DatabaseMetaData.columnNullableUnknown
        + " as NULLABLE, "
        + "'' as REMARKS, "
        + "'' as COLUMN_DEF, "
        + "0 as SQL_DATA_TYPE, "
        + "0 as SQL_DATETIME_SUB, "
        + "4 as CHAR_OCTET_LENGTH, "
        + "1 as ORDINAL_POSITION, "
        + "'YES' as IS_NULLABLE, "
        + "'' as SCOPE_CATALOG,"
        + "'' as SCOPE_SCHEMA, "
        + "'' as SCOPE_TABLE, "
        + "'' as SOURCE_DATA_TYPE, "
        + "'' as IS_AUTOINCREMENT, "
        + "'' as IS_GENERATEDCOLUMN "
        + "FROM INFORMATION_SCHEMA.COLUMNS "
        + "WHERE 1=1 ");

    if(catalog != null){
      sb.append(" AND TABLE_CATALOG = '" + StringEscapeUtils.escapeSql(catalog) + "' ");
    }
    if(schemaPattern.s != null){
      sb.append(" AND TABLE_SCHEMA like '" + StringEscapeUtils.escapeSql(schemaPattern.s) + "'");
    }

    if(tableNamePattern.s != null){
      sb.append(" AND TABLE_NAME like '" + StringEscapeUtils.escapeSql(tableNamePattern.s) + "'");
    }

    if(columnNamePattern.s != null){
      sb.append(" AND COLUMN_NAME like '" + StringEscapeUtils.escapeSql(columnNamePattern.s) + "'");
    }

    sb.append(" ORDER BY TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME");

    return s(sb.toString());
  }

  public ResultSet getSchemas(String catalog, Pat schemaPattern) {
    StringBuilder sb = new StringBuilder();
    sb.append("select "
        + "SCHEMA_NAME as TABLE_SCHEM, "
        + "CATALOG_NAME as TABLE_CAT "
        + " FROM INFORMATION_SCHEMA.SCHEMATA WHERE 1=1 ");

    if(catalog != null){
      sb.append(" AND CATALOG_NAME = '" + StringEscapeUtils.escapeSql(catalog) + "' ");
    }
    if(schemaPattern.s != null){
      sb.append(" AND SCHEMA_NAME like '" + StringEscapeUtils.escapeSql(schemaPattern.s) + "'");
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
    return ((DrillResultSet) resultSet_).cursor;
  }

  public AvaticaPrepareResult prepare(AvaticaStatement statement_, String sql) {
    //DrillStatement statement = (DrillStatement) statement_;
    return new DrillPrepareResult(sql);
  }

  interface Named {
    String getName();
  }

}