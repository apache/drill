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

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

import org.apache.drill.jdbc.AlreadyClosedSqlException;
import org.apache.drill.jdbc.DrillDatabaseMetaData;

import net.hydromatic.avatica.AvaticaConnection;
import net.hydromatic.avatica.AvaticaDatabaseMetaData;


/**
 * Drill's implementation of {@link DatabaseMetaData}.
 */
class DrillDatabaseMetaDataImpl extends AvaticaDatabaseMetaData
                                implements DrillDatabaseMetaData {

  protected DrillDatabaseMetaDataImpl( AvaticaConnection connection ) {
    super( connection );
  }

  /**
   * Throws AlreadyClosedSqlException if the associated Connection is closed.
   *
   * @throws AlreadyClosedSqlException if Connection is closed
   * @throws SQLException if error in calling {@link Connection#isClosed()}
   */
  private void throwIfClosed() throws AlreadyClosedSqlException,
                                      SQLException {
    if ( getConnection().isClosed() ) {
      throw new AlreadyClosedSqlException(
          "DatabaseMetaData's Connection is already closed." );
    }
  }


  // Note:  Dynamic proxies could be used to reduce the quantity (450?) of
  // method overrides by eliminating those that exist solely to check whether
  // the object is closed.  (Check performance before applying to frequently
  // called ResultSet.)

  // Note:  Methods are in same order as in java.sql.DatabaseMetaData.

  // No isWrapperFor(Class<?>) (it doesn't throw SQLException if already closed).
  // No unwrap(Class<T>) (it doesn't throw SQLException if already closed).

  @Override
  public boolean allProceduresAreCallable() throws SQLException {
    throwIfClosed();
    return super.allProceduresAreCallable();
  }

  @Override
  public boolean allTablesAreSelectable() throws SQLException {
    throwIfClosed();
    return super.allTablesAreSelectable();
  }

  @Override
  public String getURL() throws SQLException {
    throwIfClosed();
    return super.getURL();
  }

  @Override
  public String getUserName() throws SQLException {
    throwIfClosed();
    return super.getUserName();
  }

  @Override
  public boolean isReadOnly() throws SQLException {
    throwIfClosed();
    return super.isReadOnly();
  }


  // For omitted NULLS FIRST/NULLS HIGH, Drill sort NULL sorts as highest value:

  @Override
  public boolean nullsAreSortedHigh() throws SQLException {
    throwIfClosed();
    return true;
  }

  @Override
  public boolean nullsAreSortedLow() throws SQLException {
    throwIfClosed();
    return false;
  }

  @Override
  public boolean nullsAreSortedAtStart() throws SQLException {
    throwIfClosed();
    return false;
  }

  @Override
  public boolean nullsAreSortedAtEnd() throws SQLException {
    throwIfClosed();
    return false;
  }

  @Override
  public String getDatabaseProductName() throws SQLException {
    throwIfClosed();
    return super.getDatabaseProductName();
  }

  @Override
  public String getDatabaseProductVersion() throws SQLException {
    throwIfClosed();
    return super.getDatabaseProductVersion();
  }

  @Override
  public String getDriverName() throws SQLException {
    throwIfClosed();
    return super.getDriverName();
  }

  @Override
  public String getDriverVersion() throws SQLException {
    throwIfClosed();
    return super.getDriverVersion();
  }

  @Override
  public int getDriverMajorVersion() {
    // No already-closed exception required or allowed by JDBC.
    return super.getDriverMajorVersion();
  }

  @Override
  public int getDriverMinorVersion() {
    // No already-closed exception required or allowed by JDBC.
    return super.getDriverMinorVersion();
  }

  @Override
  public boolean usesLocalFiles() throws SQLException {
    throwIfClosed();
    return super.usesLocalFiles();
  }

  @Override
  public boolean usesLocalFilePerTable() throws SQLException {
    throwIfClosed();
    return super.usesLocalFilePerTable();
  }

  @Override
  public boolean supportsMixedCaseIdentifiers() throws SQLException {
    throwIfClosed();
    return super.supportsMixedCaseIdentifiers();
  }

  @Override
  public boolean storesUpperCaseIdentifiers() throws SQLException {
    throwIfClosed();
    return super.storesUpperCaseIdentifiers();
  }

  @Override
  public boolean storesLowerCaseIdentifiers() throws SQLException {
    throwIfClosed();
    return super.storesLowerCaseIdentifiers();
  }

  @Override
  public boolean storesMixedCaseIdentifiers() throws SQLException {
    throwIfClosed();
    return super.storesMixedCaseIdentifiers();
  }

  @Override
  public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
    throwIfClosed();
    return super.supportsMixedCaseQuotedIdentifiers();
  }

  @Override
  public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
    throwIfClosed();
    return super.storesUpperCaseQuotedIdentifiers();
  }

  @Override
  public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
    throwIfClosed();
    return super.storesLowerCaseQuotedIdentifiers();
  }

  @Override
  public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
    throwIfClosed();
    return super.storesMixedCaseQuotedIdentifiers();
  }

  // TODO(DRILL-3510):  Update when Drill accepts standard SQL's double quote.
  @Override
  public String getIdentifierQuoteString() throws SQLException {
    throwIfClosed();
    return "`";
  }

  @Override
  public String getSQLKeywords() throws SQLException {
    throwIfClosed();
    return super.getSQLKeywords();
  }

  @Override
  public String getNumericFunctions() throws SQLException {
    throwIfClosed();
    return super.getNumericFunctions();
  }

  @Override
  public String getStringFunctions() throws SQLException {
    throwIfClosed();
    return super.getStringFunctions();
  }

  @Override
  public String getSystemFunctions() throws SQLException {
    throwIfClosed();
    return super.getSystemFunctions();
  }

  @Override
  public String getTimeDateFunctions() throws SQLException {
    throwIfClosed();
    return super.getTimeDateFunctions();
  }

  @Override
  public String getSearchStringEscape() throws SQLException {
    throwIfClosed();
    return super.getSearchStringEscape();
  }

  @Override
  public String getExtraNameCharacters() throws SQLException {
    throwIfClosed();
    return super.getExtraNameCharacters();
  }

  @Override
  public boolean supportsAlterTableWithAddColumn() throws SQLException {
    throwIfClosed();
    return super.supportsAlterTableWithAddColumn();
  }

  @Override
  public boolean supportsAlterTableWithDropColumn() throws SQLException {
    throwIfClosed();
    return super.supportsAlterTableWithDropColumn();
  }

  @Override
  public boolean supportsColumnAliasing() throws SQLException {
    throwIfClosed();
    return super.supportsColumnAliasing();
  }

  @Override
  public boolean nullPlusNonNullIsNull() throws SQLException {
    throwIfClosed();
    return super.nullPlusNonNullIsNull();
  }

  @Override
  public boolean supportsConvert() throws SQLException {
    throwIfClosed();
    return super.supportsConvert();
  }

  @Override
  public boolean supportsConvert(int fromType, int toType) throws SQLException {
    throwIfClosed();
    return super.supportsConvert(fromType, toType);
  }

  @Override
  public boolean supportsTableCorrelationNames() throws SQLException {
    throwIfClosed();
    return super.supportsTableCorrelationNames();
  }

  @Override
  public boolean supportsDifferentTableCorrelationNames() throws SQLException {
    throwIfClosed();
    return super.supportsDifferentTableCorrelationNames();
  }

  @Override
  public boolean supportsExpressionsInOrderBy() throws SQLException {
    throwIfClosed();
    return super.supportsExpressionsInOrderBy();
  }

  @Override
  public boolean supportsOrderByUnrelated() throws SQLException {
    throwIfClosed();
    return super.supportsOrderByUnrelated();
  }

  @Override
  public boolean supportsGroupBy() throws SQLException {
    throwIfClosed();
    return super.supportsGroupBy();
  }

  @Override
  public boolean supportsGroupByUnrelated() throws SQLException {
    throwIfClosed();
    return super.supportsGroupByUnrelated();
  }

  @Override
  public boolean supportsGroupByBeyondSelect() throws SQLException {
    throwIfClosed();
    return super.supportsGroupByBeyondSelect();
  }

  @Override
  public boolean supportsLikeEscapeClause() throws SQLException {
    throwIfClosed();
    return super.supportsLikeEscapeClause();
  }

  @Override
  public boolean supportsMultipleResultSets() throws SQLException {
    throwIfClosed();
    return super.supportsMultipleResultSets();
  }

  @Override
  public boolean supportsMultipleTransactions() throws SQLException {
    throwIfClosed();
    return super.supportsMultipleTransactions();
  }

  @Override
  public boolean supportsNonNullableColumns() throws SQLException {
    throwIfClosed();
    return super.supportsNonNullableColumns();
  }

  @Override
  public boolean supportsMinimumSQLGrammar() throws SQLException {
    throwIfClosed();
    return super.supportsMinimumSQLGrammar();
  }

  @Override
  public boolean supportsCoreSQLGrammar() throws SQLException {
    throwIfClosed();
    return super.supportsCoreSQLGrammar();
  }

  @Override
  public boolean supportsExtendedSQLGrammar() throws SQLException {
    throwIfClosed();
    return super.supportsExtendedSQLGrammar();
  }

  @Override
  public boolean supportsANSI92EntryLevelSQL() throws SQLException {
    throwIfClosed();
    return super.supportsANSI92EntryLevelSQL();
  }

  @Override
  public boolean supportsANSI92IntermediateSQL() throws SQLException {
    throwIfClosed();
    return super.supportsANSI92IntermediateSQL();
  }

  @Override
  public boolean supportsANSI92FullSQL() throws SQLException {
    throwIfClosed();
    return super.supportsANSI92FullSQL();
  }

  @Override
  public boolean supportsIntegrityEnhancementFacility() throws SQLException {
    throwIfClosed();
    return super.supportsIntegrityEnhancementFacility();
  }

  @Override
  public boolean supportsOuterJoins() throws SQLException {
    throwIfClosed();
    return super.supportsOuterJoins();
  }

  @Override
  public boolean supportsFullOuterJoins() throws SQLException {
    throwIfClosed();
    return super.supportsFullOuterJoins();
  }

  @Override
  public boolean supportsLimitedOuterJoins() throws SQLException {
    throwIfClosed();
    return super.supportsLimitedOuterJoins();
  }

  @Override
  public String getSchemaTerm() throws SQLException {
    throwIfClosed();
    return super.getSchemaTerm();
  }

  @Override
  public String getProcedureTerm() throws SQLException {
    throwIfClosed();
    return super.getProcedureTerm();
  }

  @Override
  public String getCatalogTerm() throws SQLException {
    throwIfClosed();
    return super.getCatalogTerm();
  }

  @Override
  public boolean isCatalogAtStart() throws SQLException {
    throwIfClosed();
    return super.isCatalogAtStart();
  }

  @Override
  public String getCatalogSeparator() throws SQLException {
    throwIfClosed();
    return super.getCatalogSeparator();
  }

  @Override
  public boolean supportsSchemasInDataManipulation() throws SQLException {
    throwIfClosed();
    return super.supportsSchemasInDataManipulation();
  }

  @Override
  public boolean supportsSchemasInProcedureCalls() throws SQLException {
    throwIfClosed();
    return super.supportsSchemasInProcedureCalls();
  }

  @Override
  public boolean supportsSchemasInTableDefinitions() throws SQLException {
    throwIfClosed();
    return super.supportsSchemasInTableDefinitions();
  }

  @Override
  public boolean supportsSchemasInIndexDefinitions() throws SQLException {
    throwIfClosed();
    return super.supportsSchemasInIndexDefinitions();
  }

  @Override
  public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
    throwIfClosed();
    return super.supportsSchemasInPrivilegeDefinitions();
  }

  @Override
  public boolean supportsCatalogsInDataManipulation() throws SQLException {
    throwIfClosed();
    return super.supportsCatalogsInDataManipulation();
  }

  @Override
  public boolean supportsCatalogsInProcedureCalls() throws SQLException {
    throwIfClosed();
    return super.supportsCatalogsInProcedureCalls();
  }

  @Override
  public boolean supportsCatalogsInTableDefinitions() throws SQLException {
    throwIfClosed();
    return super.supportsCatalogsInTableDefinitions();
  }

  @Override
  public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
    throwIfClosed();
    return super.supportsCatalogsInIndexDefinitions();
  }

  @Override
  public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
    throwIfClosed();
    return super.supportsCatalogsInPrivilegeDefinitions();
  }

  @Override
  public boolean supportsPositionedDelete() throws SQLException {
    throwIfClosed();
    return super.supportsPositionedDelete();
  }

  @Override
  public boolean supportsPositionedUpdate() throws SQLException {
    throwIfClosed();
    return super.supportsPositionedUpdate();
  }

  @Override
  public boolean supportsSelectForUpdate() throws SQLException {
    throwIfClosed();
    return super.supportsSelectForUpdate();
  }

  @Override
  public boolean supportsStoredProcedures() throws SQLException {
    throwIfClosed();
    return super.supportsStoredProcedures();
  }

  @Override
  public boolean supportsSubqueriesInComparisons() throws SQLException {
    throwIfClosed();
    return super.supportsSubqueriesInComparisons();
  }

  @Override
  public boolean supportsSubqueriesInExists() throws SQLException {
    throwIfClosed();
    return super.supportsSubqueriesInExists();
  }

  @Override
  public boolean supportsSubqueriesInIns() throws SQLException {
    throwIfClosed();
    return super.supportsSubqueriesInIns();
  }

  @Override
  public boolean supportsSubqueriesInQuantifieds() throws SQLException {
    throwIfClosed();
    return super.supportsSubqueriesInQuantifieds();
  }

  @Override
  public boolean supportsCorrelatedSubqueries() throws SQLException {
    throwIfClosed();
    return super.supportsCorrelatedSubqueries();
  }

  @Override
  public boolean supportsUnion() throws SQLException {
    throwIfClosed();
    return super.supportsUnion();
  }

  @Override
  public boolean supportsUnionAll() throws SQLException {
    throwIfClosed();
    return super.supportsUnionAll();
  }

  @Override
  public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
    throwIfClosed();
    return super.supportsOpenCursorsAcrossCommit();
  }

  @Override
  public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
    throwIfClosed();
    return super.supportsOpenCursorsAcrossRollback();
  }

  @Override
  public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
    throwIfClosed();
    return super.supportsOpenStatementsAcrossCommit();
  }

  @Override
  public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
    throwIfClosed();
    return super.supportsOpenStatementsAcrossRollback();
  }

  @Override
  public int getMaxBinaryLiteralLength() throws SQLException {
    throwIfClosed();
    return super.getMaxBinaryLiteralLength();
  }

  @Override
  public int getMaxCharLiteralLength() throws SQLException {
    throwIfClosed();
    return super.getMaxCharLiteralLength();
  }

  @Override
  public int getMaxColumnNameLength() throws SQLException {
    throwIfClosed();
    return super.getMaxColumnNameLength();
  }

  @Override
  public int getMaxColumnsInGroupBy() throws SQLException {
    throwIfClosed();
    return super.getMaxColumnsInGroupBy();
  }

  @Override
  public int getMaxColumnsInIndex() throws SQLException {
    throwIfClosed();
    return super.getMaxColumnsInIndex();
  }

  @Override
  public int getMaxColumnsInOrderBy() throws SQLException {
    throwIfClosed();
    return super.getMaxColumnsInOrderBy();
  }

  @Override
  public int getMaxColumnsInSelect() throws SQLException {
    throwIfClosed();
    return super.getMaxColumnsInSelect();
  }

  @Override
  public int getMaxColumnsInTable() throws SQLException {
    throwIfClosed();
    return super.getMaxColumnsInTable();
  }

  @Override
  public int getMaxConnections() throws SQLException {
    throwIfClosed();
    return super.getMaxConnections();
  }

  @Override
  public int getMaxCursorNameLength() throws SQLException {
    throwIfClosed();
    return super.getMaxCursorNameLength();
  }

  @Override
  public int getMaxIndexLength() throws SQLException {
    throwIfClosed();
    return super.getMaxIndexLength();
  }

  @Override
  public int getMaxSchemaNameLength() throws SQLException {
    throwIfClosed();
    return super.getMaxSchemaNameLength();
  }

  @Override
  public int getMaxProcedureNameLength() throws SQLException {
    throwIfClosed();
    return super.getMaxProcedureNameLength();
  }

  @Override
  public int getMaxCatalogNameLength() throws SQLException {
    throwIfClosed();
    return super.getMaxCatalogNameLength();
  }

  @Override
  public int getMaxRowSize() throws SQLException {
    throwIfClosed();
    return super.getMaxRowSize();
  }

  @Override
  public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
    throwIfClosed();
    return super.doesMaxRowSizeIncludeBlobs();
  }

  @Override
  public int getMaxStatementLength() throws SQLException {
    throwIfClosed();
    return super.getMaxStatementLength();
  }

  @Override
  public int getMaxStatements() throws SQLException {
    throwIfClosed();
    return super.getMaxStatements();
  }

  @Override
  public int getMaxTableNameLength() throws SQLException {
    throwIfClosed();
    return super.getMaxTableNameLength();
  }

  @Override
  public int getMaxTablesInSelect() throws SQLException {
    throwIfClosed();
    return super.getMaxTablesInSelect();
  }

  @Override
  public int getMaxUserNameLength() throws SQLException {
    throwIfClosed();
    return super.getMaxUserNameLength();
  }

  @Override
  public int getDefaultTransactionIsolation() throws SQLException {
    throwIfClosed();
    return super.getDefaultTransactionIsolation();
  }

  @Override
  public boolean supportsTransactions() throws SQLException {
    throwIfClosed();
    return super.supportsTransactions();
  }

  @Override
  public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
    throwIfClosed();
    return super.supportsTransactionIsolationLevel(level);
  }

  @Override
  public boolean supportsDataDefinitionAndDataManipulationTransactions()
      throws SQLException {
    throwIfClosed();
    return super.supportsDataDefinitionAndDataManipulationTransactions();
  }

  @Override
  public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
    throwIfClosed();
    return super.supportsDataManipulationTransactionsOnly();
  }

  @Override
  public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
    throwIfClosed();
    return super.dataDefinitionCausesTransactionCommit();
  }

  @Override
  public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
    throwIfClosed();
    return super.dataDefinitionIgnoredInTransactions();
  }

  @Override
  public ResultSet getProcedures(String catalog, String schemaPattern,
                                 String procedureNamePattern) throws SQLException {
    throwIfClosed();
    return super.getProcedures(catalog, schemaPattern, procedureNamePattern);
  }

  @Override
  public ResultSet getProcedureColumns(String catalog, String schemaPattern,
                                       String procedureNamePattern,
                                       String columnNamePattern) throws SQLException {
    throwIfClosed();
    return super.getProcedureColumns(catalog, schemaPattern,
                                     procedureNamePattern, columnNamePattern);
  }

  @Override
  public ResultSet getTables(String catalog,
                             String schemaPattern,
                             String tableNamePattern,
                             String[] types) throws SQLException {
    throwIfClosed();
    return super.getTables(catalog, schemaPattern,tableNamePattern, types);
  }


  @Override
  public ResultSet getSchemas() throws SQLException {
    throwIfClosed();
    return super.getSchemas();
  }

  @Override
  public ResultSet getCatalogs() throws SQLException {
    throwIfClosed();
    return super.getCatalogs();
  }

  @Override
  public ResultSet getTableTypes() throws SQLException {
    throwIfClosed();
    return super.getTableTypes();
  }

  @Override
  public ResultSet getColumns(String catalog, String schema, String table,
                              String columnNamePattern) throws SQLException {
    throwIfClosed();
    return super.getColumns(catalog, schema, table, columnNamePattern);
  }

  @Override
  public ResultSet getColumnPrivileges(String catalog, String schema,
                                       String table,
                                       String columnNamePattern) throws SQLException {
    throwIfClosed();
    return super.getColumnPrivileges(catalog, schema, table, columnNamePattern);
  }

  @Override
  public ResultSet getTablePrivileges(String catalog, String schemaPattern,
                                      String tableNamePattern) throws SQLException {
    throwIfClosed();
    return super.getTablePrivileges(catalog, schemaPattern, tableNamePattern);
  }

  @Override
  public ResultSet getBestRowIdentifier(String catalog, String schema,
                                        String table, int scope,
                                        boolean nullable) throws SQLException {
    throwIfClosed();
    return super.getBestRowIdentifier(catalog, schema, table, scope, nullable);
  }

  @Override
  public ResultSet getVersionColumns(String catalog, String schema,
                                     String table) throws SQLException {
    throwIfClosed();
    return super.getVersionColumns(catalog, schema, table);
  }

  @Override
  public ResultSet getPrimaryKeys(String catalog, String schema,
                                  String table) throws SQLException {
    throwIfClosed();
    return super.getPrimaryKeys(catalog, schema, table);
  }

  @Override
  public ResultSet getImportedKeys(String catalog, String schema,
                                   String table) throws SQLException {
    throwIfClosed();
    return super.getImportedKeys(catalog, schema, table);
  }

  @Override
  public ResultSet getExportedKeys(String catalog, String schema,
                                   String table) throws SQLException {
    throwIfClosed();
    return super.getExportedKeys(catalog, schema, table);
  }

  @Override
  public ResultSet getCrossReference(
      String parentCatalog, String parentSchema, String parentTable,
      String foreignCatalog, String foreignSchema,
      String foreignTable ) throws SQLException {
    throwIfClosed();
    return super.getCrossReference(parentCatalog, parentSchema, parentTable,
                                   foreignCatalog, foreignSchema, foreignTable );
  }

  @Override
  public ResultSet getTypeInfo() throws SQLException {
    throwIfClosed();
    return super.getTypeInfo();
  }

  @Override
  public ResultSet getIndexInfo(String catalog, String schema, String table,
                                boolean unique,
                                boolean approximate) throws SQLException {
    throwIfClosed();
    return super.getIndexInfo(catalog, schema, table, unique, approximate);
  }

  @Override
  public boolean supportsResultSetType(int type) throws SQLException {
    throwIfClosed();
    return super.supportsResultSetType(type);
  }

  @Override
  public boolean supportsResultSetConcurrency(int type,
                                              int concurrency) throws SQLException {
    throwIfClosed();
    return super.supportsResultSetConcurrency(type, concurrency);
  }

  @Override
  public boolean ownUpdatesAreVisible(int type) throws SQLException {
    throwIfClosed();
    try {
      return super.ownUpdatesAreVisible(type);
    }
    catch (RuntimeException e) {
      if ("todo: implement this method".equals(e.getMessage())) {
        throw new SQLFeatureNotSupportedException(
            "ownUpdatesAreVisible(int) is not supported", e);
      }
      else {
        throw new SQLException(e.getMessage(), e);
      }
    }
  }

  @Override
  public boolean ownDeletesAreVisible(int type) throws SQLException {
    throwIfClosed();
    try {
      return super.ownDeletesAreVisible(type);
    }
    catch (RuntimeException e) {
      if ("todo: implement this method".equals(e.getMessage())) {
        throw new SQLFeatureNotSupportedException(
            "ownDeletesAreVisible(int) is not supported", e);
      }
      else {
        throw new SQLException(e.getMessage(), e);
      }
    }
  }

  @Override
  public boolean ownInsertsAreVisible(int type) throws SQLException {
    throwIfClosed();
    try {
      return super.ownInsertsAreVisible(type);
    }
    catch (RuntimeException e) {
      if ("todo: implement this method".equals(e.getMessage())) {
        throw new SQLFeatureNotSupportedException(
            "ownInsertsAreVisible(int) is not supported", e);
      }
      else {
        throw new SQLException(e.getMessage(), e);
      }
    }
  }

  @Override
  public boolean othersUpdatesAreVisible(int type) throws SQLException {
    throwIfClosed();
    try {
      return super.othersUpdatesAreVisible(type);
    }
    catch (RuntimeException e) {
      if ("todo: implement this method".equals(e.getMessage())) {
        throw new SQLFeatureNotSupportedException(
            "othersUpdatesAreVisible(int) is not supported", e);
      }
      else {
        throw new SQLException(e.getMessage(), e);
      }
    }
  }

  @Override
  public boolean othersDeletesAreVisible(int type) throws SQLException {
    throwIfClosed();
    try {
      return super.othersDeletesAreVisible(type);
    }
    catch (RuntimeException e) {
      if ("todo: implement this method".equals(e.getMessage())) {
        throw new SQLFeatureNotSupportedException(
            "othersDeletesAreVisible(int) is not supported", e);
      }
      else {
        throw new SQLException(e.getMessage(), e);
      }
    }
  }

  @Override
  public boolean othersInsertsAreVisible(int type) throws SQLException {
    throwIfClosed();
    try {
      return super.othersInsertsAreVisible(type);
    }
    catch (RuntimeException e) {
      if ("todo: implement this method".equals(e.getMessage())) {
        throw new SQLFeatureNotSupportedException(
            "othersInsertsAreVisible(int) is not supported", e);
      }
      else {
        throw new SQLException(e.getMessage(), e);
      }
    }
  }

  @Override
  public boolean updatesAreDetected(int type) throws SQLException {
    throwIfClosed();
    try {
      return super.updatesAreDetected(type);
    }
    catch (RuntimeException e) {
      if ("todo: implement this method".equals(e.getMessage())) {
        throw new SQLFeatureNotSupportedException(
            "updatesAreDetected(int) is not supported", e);
      }
      else {
        throw new SQLException(e.getMessage(), e);
      }
    }
  }

  @Override
  public boolean deletesAreDetected(int type) throws SQLException {
    throwIfClosed();
    try {
      return super.deletesAreDetected(type);
    }
    catch (RuntimeException e) {
      if ("todo: implement this method".equals(e.getMessage())) {
        throw new SQLFeatureNotSupportedException(
            "deletesAreDetected(int) is not supported", e);
      }
      else {
        throw new SQLException(e.getMessage(), e);
      }
    }
  }

  @Override
  public boolean insertsAreDetected(int type) throws SQLException {
    throwIfClosed();
    try {
      return super.insertsAreDetected(type);
    }
    catch (RuntimeException e) {
      if ("todo: implement this method".equals(e.getMessage())) {
        throw new SQLFeatureNotSupportedException(
            "insertsAreDetected(int) is not supported", e);
      }
      else {
        throw new SQLException(e.getMessage(), e);
      }
    }
  }

  @Override
  public boolean supportsBatchUpdates() throws SQLException {
    throwIfClosed();
    return super.supportsBatchUpdates();
  }

  @Override
  public ResultSet getUDTs(String catalog, String schemaPattern,
                           String typeNamePattern,
                           int[] types) throws SQLException {
    throwIfClosed();
    return super.getUDTs(catalog, schemaPattern, typeNamePattern, types);
  }

  @Override
  public Connection getConnection() throws SQLException {
    // No already-closed exception required by JDBC.
    return super.getConnection();
  }

  @Override
  public boolean supportsSavepoints() throws SQLException {
    throwIfClosed();
    return super.supportsSavepoints();
  }

  @Override
  public boolean supportsNamedParameters() throws SQLException {
    throwIfClosed();
    return super.supportsNamedParameters();
  }

  @Override
  public boolean supportsMultipleOpenResults() throws SQLException {
    throwIfClosed();
    return super.supportsMultipleOpenResults();
  }

  @Override
  public boolean supportsGetGeneratedKeys() throws SQLException {
    throwIfClosed();
    return super.supportsGetGeneratedKeys();
  }

  @Override
  public ResultSet getSuperTypes(String catalog, String schemaPattern,
                                 String typeNamePattern) throws SQLException {
    throwIfClosed();
    return super.getSuperTypes(catalog, schemaPattern, typeNamePattern);
  }

  @Override
  public ResultSet getSuperTables(String catalog, String schemaPattern,
                                  String tableNamePattern) throws SQLException {
    throwIfClosed();
    return super.getSuperTables(catalog, schemaPattern, tableNamePattern);
  }

  @Override
  public ResultSet getAttributes(String catalog, String schemaPattern,
                                 String typeNamePattern,
                                 String attributeNamePattern) throws SQLException {
    throwIfClosed();
    return super.getAttributes(catalog, schemaPattern, typeNamePattern,
                               attributeNamePattern);
  }

  @Override
  public boolean supportsResultSetHoldability(int holdability) throws SQLException {
    throwIfClosed();
    try {
      return super.supportsResultSetHoldability(holdability);
    }
    catch (RuntimeException e) {
      if ("todo: implement this method".equals(e.getMessage())) {
        throw new SQLFeatureNotSupportedException(
            "supportsResultSetHoldability(int) is not supported", e);
      }
      else {
        throw new SQLException(e.getMessage(), e);
      }
    }
  }

  @Override
  public int getResultSetHoldability() {
    // Can't throw any SQLException because Avatica's getResultSetHoldability()
    // is missing "throws SQLException".
    try {
      throwIfClosed();
    } catch (AlreadyClosedSqlException e) {
      throw new RuntimeException(e.getMessage(), e);
    } catch (SQLException e) {
      throw new RuntimeException(e.getMessage(), e);
    }
    return super.getResultSetHoldability();
  }

  @Override
  public int getDatabaseMajorVersion() throws SQLException {
    throwIfClosed();
    return super.getDatabaseMajorVersion();
  }

  @Override
  public int getDatabaseMinorVersion() throws SQLException {
    throwIfClosed();
    return super.getDatabaseMinorVersion();
  }

  @Override
  public int getJDBCMajorVersion() throws SQLException {
    throwIfClosed();
    return super.getJDBCMajorVersion();
  }

  @Override
  public int getJDBCMinorVersion() throws SQLException {
    throwIfClosed();
    return super.getJDBCMinorVersion();
  }

  @Override
  public int getSQLStateType() throws SQLException {
    throwIfClosed();
    return super.getSQLStateType();
  }

  @Override
  public boolean locatorsUpdateCopy() throws SQLException {
    throwIfClosed();
    return super.locatorsUpdateCopy();
  }

  @Override
  public boolean supportsStatementPooling() throws SQLException {
    throwIfClosed();
    return super.supportsStatementPooling();
  }

  @Override
  public RowIdLifetime getRowIdLifetime() throws SQLException {
    throwIfClosed();
    return super.getRowIdLifetime();
  }

  @Override
  public ResultSet getSchemas(String catalog,
                              String schemaPattern) throws SQLException {
    throwIfClosed();
    return super.getSchemas(catalog, schemaPattern);
  }

  @Override
  public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
    throwIfClosed();
    return super.supportsStoredFunctionsUsingCallSyntax();
  }

  @Override
  public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
    throwIfClosed();
    return super.autoCommitFailureClosesAllResultSets();
  }

  @Override
  public ResultSet getClientInfoProperties() throws SQLException {
    throwIfClosed();
    return super.getClientInfoProperties();
  }

  @Override
  public ResultSet getFunctions(String catalog, String schemaPattern,
                                String functionNamePattern) throws SQLException {
    throwIfClosed();
    return super.getFunctions(catalog, schemaPattern, functionNamePattern);
  }

  @Override
  public ResultSet getFunctionColumns(String catalog, String schemaPattern,
                                      String functionNamePattern,
                                      String columnNamePattern) throws SQLException {
    throwIfClosed();
    return super.getFunctionColumns(catalog, schemaPattern, functionNamePattern,
                                    columnNamePattern);
  }

  @Override
  public ResultSet getPseudoColumns(String catalog, String schemaPattern,
                                    String tableNamePattern,
                                    String columnNamePattern) throws SQLException {
    throwIfClosed();
    return super.getPseudoColumns(catalog, schemaPattern, tableNamePattern,
                                  columnNamePattern);
  }

  @Override
  public boolean generatedKeyAlwaysReturned() throws SQLException {
    throwIfClosed();
    return super.generatedKeyAlwaysReturned();
  }


}
