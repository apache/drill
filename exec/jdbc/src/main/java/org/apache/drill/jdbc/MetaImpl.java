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

import java.sql.ResultSet;
import java.util.List;

import net.hydromatic.avatica.AvaticaPrepareResult;
import net.hydromatic.avatica.AvaticaResultSet;
import net.hydromatic.avatica.AvaticaStatement;
import net.hydromatic.avatica.Cursor;
import net.hydromatic.avatica.Meta;
import net.hydromatic.linq4j.Linq4j;

public class MetaImpl implements Meta {
  
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

  public ResultSet getTables(String catalog, final Pat schemaPattern, final Pat tableNamePattern,
      final List<String> typeList) {
    return getEmptyResultSet();
  }

  public ResultSet getColumns(String catalog, Pat schemaPattern, Pat tableNamePattern, Pat columnNamePattern) {
    return getEmptyResultSet();
  }

  public ResultSet getSchemas(String catalog, Pat schemaPattern) {
    return getEmptyResultSet();
  }

  public ResultSet getCatalogs() {
    return getEmptyResultSet();
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