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
package org.apache.drill.jdbc.impl;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.calcite.avatica.AvaticaDatabaseMetaData;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.drill.common.Version;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.client.ServerMethod;
import org.apache.drill.exec.proto.UserBitShared.DrillPBError;
import org.apache.drill.exec.proto.UserProtos.ConvertSupport;
import org.apache.drill.exec.proto.UserProtos.CorrelationNamesSupport;
import org.apache.drill.exec.proto.UserProtos.GetServerMetaResp;
import org.apache.drill.exec.proto.UserProtos.GroupBySupport;
import org.apache.drill.exec.proto.UserProtos.IdentifierCasing;
import org.apache.drill.exec.proto.UserProtos.NullCollation;
import org.apache.drill.exec.proto.UserProtos.OrderBySupport;
import org.apache.drill.exec.proto.UserProtos.OuterJoinSupport;
import org.apache.drill.exec.proto.UserProtos.RequestStatus;
import org.apache.drill.exec.proto.UserProtos.ServerMeta;
import org.apache.drill.exec.proto.UserProtos.SubQuerySupport;
import org.apache.drill.exec.proto.UserProtos.UnionSupport;
import org.apache.drill.jdbc.AlreadyClosedSqlException;
import org.apache.drill.jdbc.DrillDatabaseMetaData;

import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;


/**
 * Drill's implementation of {@link DatabaseMetaData}.
 */
class DrillDatabaseMetaDataImpl extends AvaticaDatabaseMetaData
                                implements DrillDatabaseMetaData {


  /**
   * Holds allowed conversion between SQL types
   *
   */
  private static final class SQLConvertSupport {
    public final int from;
    public final int to;

    public SQLConvertSupport(int from, int to) {
      this.from = from;
      this.to = to;
    }

    @Override
    public int hashCode() {
      return Objects.hash(from, to);
    }

    @Override public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }

      if (!(obj instanceof SQLConvertSupport)) {
        return false;
      }

      SQLConvertSupport other = (SQLConvertSupport) obj;
      return from == other.from && to == other.to;
    }

    public static final Set<SQLConvertSupport> toSQLConvertSupport(Iterable<ConvertSupport> convertSupportIterable) {
      ImmutableSet.Builder<SQLConvertSupport> sqlConvertSupportSet = ImmutableSet.builder();
      for(ConvertSupport convertSupport: convertSupportIterable) {
        try {
          sqlConvertSupportSet.add(new SQLConvertSupport(
              toSQLType(convertSupport.getFrom()),
              toSQLType(convertSupport.getTo())));
        } catch(IllegalArgumentException e) {
          // Ignore unknown types...
        }
      }
      return sqlConvertSupportSet.build();
    }

    private static int toSQLType(MinorType minorType) {
      String sqlTypeName = Types.getSqlTypeName(Types.optional(minorType));
      return Types.getJdbcTypeCode(sqlTypeName);
    }
  }

  private volatile ServerMeta serverMeta;
  private volatile Set<SQLConvertSupport> convertSupport;

  protected DrillDatabaseMetaDataImpl( DrillConnectionImpl connection ) {
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

  private boolean getServerMetaSupported() throws SQLException {
    DrillConnectionImpl connection = (DrillConnectionImpl) getConnection();
    return
        !connection.getConfig().isServerMetadataDisabled()
        && connection.getClient().getSupportedMethods().contains(ServerMethod.GET_SERVER_META);
  }

  private String getServerName() throws SQLException {
    DrillConnectionImpl connection = (DrillConnectionImpl) getConnection();
    return connection.getClient().getServerName();
  }

  private Version getServerVersion() throws SQLException {
    DrillConnectionImpl connection = (DrillConnectionImpl) getConnection();
    return connection.getClient().getServerVersion();
  }

  private ServerMeta getServerMeta() throws SQLException {
    assert getServerMetaSupported();

    if (serverMeta == null) {
      synchronized(this) {
        if (serverMeta == null) {
          DrillConnectionImpl connection = (DrillConnectionImpl) getConnection();

          try {
            GetServerMetaResp resp = connection.getClient().getServerMeta().get();
            if (resp.getStatus() != RequestStatus.OK) {
              DrillPBError drillError = resp.getError();
              throw new SQLException("Error when getting server meta: " + drillError.getMessage());
            }
            serverMeta = resp.getServerMeta();
            convertSupport = SQLConvertSupport.toSQLConvertSupport(serverMeta.getConvertSupportList());
          } catch (InterruptedException e) {
            throw new SQLException("Interrupted when getting server meta", e);
          } catch (ExecutionException e) {
            Throwable cause =  e.getCause();
            if (cause == null) {
              throw new AssertionError("Something unknown happened", e);
            }
            Throwables.propagateIfPossible(cause);
            throw new SQLException("Error when getting server meta", cause);
          }
        }
      }
    }

    return serverMeta;
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
    if (!getServerMetaSupported()) {
      return super.allTablesAreSelectable();
    }
    return getServerMeta().getAllTablesSelectable();
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
    if (!getServerMetaSupported()) {
      return super.isReadOnly();
    }
    return getServerMeta().getReadOnly();
  }


  // For omitted NULLS FIRST/NULLS HIGH, Drill sort NULL sorts as highest value:

  @Override
  public boolean nullsAreSortedHigh() throws SQLException {
    throwIfClosed();
    if (!getServerMetaSupported()) {
      return true;
    }
    return getServerMeta().getNullCollation() == NullCollation.NC_HIGH;
  }

  @Override
  public boolean nullsAreSortedLow() throws SQLException {
    throwIfClosed();
    if (!getServerMetaSupported()) {
      return false;
    }
    return getServerMeta().getNullCollation() == NullCollation.NC_LOW;
  }

  @Override
  public boolean nullsAreSortedAtStart() throws SQLException {
    throwIfClosed();
    if (!getServerMetaSupported()) {
      return false;
    }
    return getServerMeta().getNullCollation() == NullCollation.NC_AT_START;
  }

  @Override
  public boolean nullsAreSortedAtEnd() throws SQLException {
    throwIfClosed();
    if (!getServerMetaSupported()) {
      return false;
    }
    return getServerMeta().getNullCollation() == NullCollation.NC_AT_END;
  }

  @Override
  public String getDatabaseProductName() throws SQLException {
    throwIfClosed();
    String name = getServerName();
    if (name == null) {
      return super.getDatabaseProductName();
    }
    return name;
  }

  @Override
  public String getDatabaseProductVersion() throws SQLException {
    throwIfClosed();
    Version version = getServerVersion();
    if (version == null) {
      return super.getDatabaseProductVersion();
    }
    return version.getVersion();
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
    if (!getServerMetaSupported()) {
      return super.supportsMixedCaseIdentifiers();
    }
    return getServerMeta().getIdentifierCasing() == IdentifierCasing.IC_SUPPORTS_MIXED;
  }

  @Override
  public boolean storesUpperCaseIdentifiers() throws SQLException {
    throwIfClosed();
    if (!getServerMetaSupported()) {
      return super.storesUpperCaseIdentifiers();
    }
    return getServerMeta().getIdentifierCasing() == IdentifierCasing.IC_STORES_UPPER;
  }

  @Override
  public boolean storesLowerCaseIdentifiers() throws SQLException {
    throwIfClosed();
    if (!getServerMetaSupported()) {
      return super.storesLowerCaseIdentifiers();
    }
    return getServerMeta().getIdentifierCasing() == IdentifierCasing.IC_STORES_LOWER;
  }

  @Override
  public boolean storesMixedCaseIdentifiers() throws SQLException {
    throwIfClosed();
    if (!getServerMetaSupported()) {
      return super.storesMixedCaseIdentifiers();
    }
    return getServerMeta().getIdentifierCasing() == IdentifierCasing.IC_STORES_MIXED;
  }

  @Override
  public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
    throwIfClosed();
    if (!getServerMetaSupported()) {
      return super.supportsMixedCaseQuotedIdentifiers();
    }
    return getServerMeta().getQuotedIdentifierCasing() == IdentifierCasing.IC_SUPPORTS_MIXED;
  }

  @Override
  public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
    throwIfClosed();
    if (!getServerMetaSupported()) {
      return super.storesUpperCaseQuotedIdentifiers();
    }
    return getServerMeta().getQuotedIdentifierCasing() == IdentifierCasing.IC_STORES_UPPER;
  }

  @Override
  public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
    throwIfClosed();
    if (!getServerMetaSupported()) {
      return super.storesLowerCaseQuotedIdentifiers();
    }
    return getServerMeta().getQuotedIdentifierCasing() == IdentifierCasing.IC_STORES_LOWER;
  }

  @Override
  public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
    throwIfClosed();
    if (!getServerMetaSupported()) {
      return super.storesMixedCaseQuotedIdentifiers();
    }
    return getServerMeta().getQuotedIdentifierCasing() == IdentifierCasing.IC_STORES_MIXED;
  }

  @Override
  public String getIdentifierQuoteString() throws SQLException {
    throwIfClosed();
    if (!getServerMetaSupported()) {
      return Quoting.BACK_TICK.string;
    }
    return getServerMeta().getIdentifierQuoteString();
  }

  @Override
  public String getSQLKeywords() throws SQLException {
    throwIfClosed();
    if (!getServerMetaSupported()) {
      return super.getSQLKeywords();
    }
    return Joiner.on(",").join(getServerMeta().getSqlKeywordsList());
  }

  @Override
  public String getNumericFunctions() throws SQLException {
    throwIfClosed();
    if (!getServerMetaSupported()) {
      return super.getNumericFunctions();
    }
    return Joiner.on(",").join(getServerMeta().getNumericFunctionsList());
  }

  @Override
  public String getStringFunctions() throws SQLException {
    throwIfClosed();
    if (!getServerMetaSupported()) {
      return super.getStringFunctions();
    }
    return Joiner.on(",").join(getServerMeta().getStringFunctionsList());
  }

  @Override
  public String getSystemFunctions() throws SQLException {
    throwIfClosed();
    if (!getServerMetaSupported()) {
      return super.getSystemFunctions();
    }
    return Joiner.on(",").join(getServerMeta().getSystemFunctionsList());
  }

  @Override
  public String getTimeDateFunctions() throws SQLException {
    throwIfClosed();
    if (!getServerMetaSupported()) {
      return super.getTimeDateFunctions();
    }
    return Joiner.on(",").join(getServerMeta().getDateTimeFunctionsList());
  }

  @Override
  public String getSearchStringEscape() throws SQLException {
    throwIfClosed();
    if (!getServerMetaSupported()) {
      return super.getSearchStringEscape();
    }
    return getServerMeta().getSearchEscapeString();
  }

  @Override
  public String getExtraNameCharacters() throws SQLException {
    throwIfClosed();
    if (!getServerMetaSupported()) {
      return super.getExtraNameCharacters();
    }
    return getServerMeta().getSpecialCharacters();
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
    if (!getServerMetaSupported()) {
      return super.supportsColumnAliasing();
    }
    return getServerMeta().getColumnAliasingSupported();
  }

  @Override
  public boolean nullPlusNonNullIsNull() throws SQLException {
    throwIfClosed();
    if (!getServerMetaSupported()) {
      return super.nullPlusNonNullIsNull();
    }
    return getServerMeta().getNullPlusNonNullEqualsNull();
  }

  @Override
  public boolean supportsConvert() throws SQLException {
    throwIfClosed();
    if (!getServerMetaSupported()) {
      return super.supportsConvert();
    }
    // Make sure the convert table is loaded
    getServerMeta();
    return !convertSupport.isEmpty();
  }

  @Override
  public boolean supportsConvert(int fromType, int toType) throws SQLException {
    throwIfClosed();
    if (!getServerMetaSupported()) {
      return super.supportsConvert(fromType, toType);
    }
    // Make sure the convert table is loaded
    getServerMeta();
    return convertSupport.contains(new SQLConvertSupport(fromType, toType));
  }

  @Override
  public boolean supportsTableCorrelationNames() throws SQLException {
    throwIfClosed();
    if (!getServerMetaSupported()) {
      return super.supportsTableCorrelationNames();
    }
    return getServerMeta().getCorrelationNamesSupport() == CorrelationNamesSupport.CN_ANY
        || getServerMeta().getCorrelationNamesSupport() == CorrelationNamesSupport.CN_DIFFERENT_NAMES;
  }

  @Override
  public boolean supportsDifferentTableCorrelationNames() throws SQLException {
    throwIfClosed();
    if (!getServerMetaSupported()) {
      return super.supportsDifferentTableCorrelationNames();
    }
    return getServerMeta().getCorrelationNamesSupport() == CorrelationNamesSupport.CN_DIFFERENT_NAMES;
  }

  @Override
  public boolean supportsExpressionsInOrderBy() throws SQLException {
    throwIfClosed();
    if (!getServerMetaSupported()) {
      return super.supportsExpressionsInOrderBy();
    }
    return getServerMeta().getOrderBySupportList().contains(OrderBySupport.OB_EXPRESSION);
  }

  @Override
  public boolean supportsOrderByUnrelated() throws SQLException {
    throwIfClosed();
    if (!getServerMetaSupported()) {
      return super.supportsOrderByUnrelated();
    }
    return getServerMeta().getOrderBySupportList().contains(OrderBySupport.OB_UNRELATED);
  }

  @Override
  public boolean supportsGroupBy() throws SQLException {
    throwIfClosed();
    if (!getServerMetaSupported()) {
      return super.supportsGroupBy();
    }
    return getServerMeta().getGroupBySupport() != GroupBySupport.GB_NONE;
  }

  @Override
  public boolean supportsGroupByUnrelated() throws SQLException {
    throwIfClosed();
    if (!getServerMetaSupported()) {
      return super.supportsGroupByUnrelated();
    }
    return getServerMeta().getGroupBySupport() == GroupBySupport.GB_UNRELATED;
  }

  @Override
  public boolean supportsGroupByBeyondSelect() throws SQLException {
    throwIfClosed();
    if (!getServerMetaSupported()) {
      return super.supportsGroupByBeyondSelect();
    }
    return getServerMeta().getGroupBySupport() == GroupBySupport.GB_BEYOND_SELECT;
  }

  @Override
  public boolean supportsLikeEscapeClause() throws SQLException {
    throwIfClosed();
    if (!getServerMetaSupported()) {
      return super.supportsLikeEscapeClause();
    }
    return getServerMeta().getLikeEscapeClauseSupported();
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
    if (!getServerMetaSupported()) {
      return super.supportsOuterJoins();
    }
    return getServerMeta().getOuterJoinSupportCount() > 0;
  }

  @Override
  public boolean supportsFullOuterJoins() throws SQLException {
    throwIfClosed();
    if (!getServerMetaSupported()) {
      return super.supportsFullOuterJoins();
    }
    return getServerMeta().getOuterJoinSupportList().contains(OuterJoinSupport.OJ_FULL);
  }

  @Override
  public boolean supportsLimitedOuterJoins() throws SQLException {
    throwIfClosed();
    if (!getServerMetaSupported()) {
      return super.supportsFullOuterJoins();
    }
    return getServerMeta().getOuterJoinSupportCount() > 0
        && !(getServerMeta().getOuterJoinSupportList().contains(OuterJoinSupport.OJ_FULL));
  }

  @Override
  public String getSchemaTerm() throws SQLException {
    throwIfClosed();
    if (!getServerMetaSupported()) {
      return super.getSchemaTerm();
    }
    return getServerMeta().getSchemaTerm();
  }

  @Override
  public String getProcedureTerm() throws SQLException {
    throwIfClosed();
    return super.getProcedureTerm();
  }

  @Override
  public String getCatalogTerm() throws SQLException {
    throwIfClosed();
    if (!getServerMetaSupported()) {
      return super.getCatalogTerm();
    }
    return getServerMeta().getCatalogTerm();
  }

  @Override
  public boolean isCatalogAtStart() throws SQLException {
    throwIfClosed();
    if (!getServerMetaSupported()) {
      return super.isCatalogAtStart();
    }
    return getServerMeta().getCatalogAtStart();
  }

  @Override
  public String getCatalogSeparator() throws SQLException {
    throwIfClosed();
    if (!getServerMetaSupported()) {
      return super.getCatalogSeparator();
    }
    return getServerMeta().getCatalogSeparator();
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
    if (!getServerMetaSupported()) {
      return super.supportsSelectForUpdate();
    }
    return getServerMeta().getSelectForUpdateSupported();
  }

  @Override
  public boolean supportsStoredProcedures() throws SQLException {
    throwIfClosed();
    return super.supportsStoredProcedures();
  }

  @Override
  public boolean supportsSubqueriesInComparisons() throws SQLException {
    throwIfClosed();
    if (!getServerMetaSupported()) {
      return super.supportsSubqueriesInComparisons();
    }
    return getServerMeta().getSubquerySupportList().contains(SubQuerySupport.SQ_IN_COMPARISON);
  }

  @Override
  public boolean supportsSubqueriesInExists() throws SQLException {
    throwIfClosed();
    if (!getServerMetaSupported()) {
      return super.supportsSubqueriesInExists();
    }
    return getServerMeta().getSubquerySupportList().contains(SubQuerySupport.SQ_IN_EXISTS);
  }

  @Override
  public boolean supportsSubqueriesInIns() throws SQLException {
    throwIfClosed();
    if (!getServerMetaSupported()) {
      return super.supportsSubqueriesInIns();
    }
    return getServerMeta().getSubquerySupportList().contains(SubQuerySupport.SQ_IN_INSERT);
  }

  @Override
  public boolean supportsSubqueriesInQuantifieds() throws SQLException {
    throwIfClosed();
    if (!getServerMetaSupported()) {
      return super.supportsSubqueriesInQuantifieds();
    }
    return getServerMeta().getSubquerySupportList().contains(SubQuerySupport.SQ_IN_QUANTIFIED);
  }

  @Override
  public boolean supportsCorrelatedSubqueries() throws SQLException {
    throwIfClosed();
    if (!getServerMetaSupported()) {
      return super.supportsCorrelatedSubqueries();
    }
    return getServerMeta().getSubquerySupportList().contains(SubQuerySupport.SQ_CORRELATED);
  }

  @Override
  public boolean supportsUnion() throws SQLException {
    throwIfClosed();
    if (!getServerMetaSupported()) {
      return super.supportsUnion();
    }
    return getServerMeta().getUnionSupportList().contains(UnionSupport.U_UNION);
  }

  @Override
  public boolean supportsUnionAll() throws SQLException {
    throwIfClosed();
    if (!getServerMetaSupported()) {
      return super.supportsUnionAll();
    }
    return getServerMeta().getUnionSupportList().contains(UnionSupport.U_UNION_ALL);
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
    if (!getServerMetaSupported()) {
      return super.getMaxBinaryLiteralLength();
    }
    return getServerMeta().getMaxBinaryLiteralLength();
  }

  @Override
  public int getMaxCharLiteralLength() throws SQLException {
    throwIfClosed();
    if (!getServerMetaSupported()) {
      return super.getMaxCharLiteralLength();
    }
    return getServerMeta().getMaxCharLiteralLength();
  }

  @Override
  public int getMaxColumnNameLength() throws SQLException {
    throwIfClosed();
    if (!getServerMetaSupported()) {
      return super.getMaxColumnNameLength();
    }
    return getServerMeta().getMaxColumnNameLength();
  }

  @Override
  public int getMaxColumnsInGroupBy() throws SQLException {
    throwIfClosed();
    if (!getServerMetaSupported()) {
      return super.getMaxColumnsInGroupBy();
    }
    return getServerMeta().getMaxColumnsInGroupBy();
  }

  @Override
  public int getMaxColumnsInIndex() throws SQLException {
    throwIfClosed();
    return super.getMaxColumnsInIndex();
  }

  @Override
  public int getMaxColumnsInOrderBy() throws SQLException {
    throwIfClosed();
    if (!getServerMetaSupported()) {
      return super.getMaxColumnsInOrderBy();
    }
    return getServerMeta().getMaxColumnsInOrderBy();
  }

  @Override
  public int getMaxColumnsInSelect() throws SQLException {
    throwIfClosed();
    if (!getServerMetaSupported()) {
      return super.getMaxColumnsInSelect();
    }
    return getServerMeta().getMaxColumnsInSelect();
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
    if (!getServerMetaSupported()) {
      return super.getMaxCursorNameLength();
    }
    return getServerMeta().getMaxCursorNameLength();
  }

  @Override
  public int getMaxIndexLength() throws SQLException {
    throwIfClosed();
    return super.getMaxIndexLength();
  }

  @Override
  public int getMaxSchemaNameLength() throws SQLException {
    throwIfClosed();
    if (!getServerMetaSupported()) {
      return super.getMaxSchemaNameLength();
    }
    return getServerMeta().getMaxSchemaNameLength();
  }

  @Override
  public int getMaxProcedureNameLength() throws SQLException {
    throwIfClosed();
    return super.getMaxProcedureNameLength();
  }

  @Override
  public int getMaxCatalogNameLength() throws SQLException {
    throwIfClosed();
    if (!getServerMetaSupported()) {
      return super.getMaxCatalogNameLength();
    }
    return getServerMeta().getMaxCatalogNameLength();
  }

  @Override
  public int getMaxRowSize() throws SQLException {
    throwIfClosed();
    if (!getServerMetaSupported()) {
      return super.getMaxRowSize();
    }
    return getServerMeta().getMaxRowSize();
  }

  @Override
  public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
    throwIfClosed();
    if (!getServerMetaSupported()) {
      return super.doesMaxRowSizeIncludeBlobs();
    }
    return getServerMeta().getBlobIncludedInMaxRowSize();
  }

  @Override
  public int getMaxStatementLength() throws SQLException {
    throwIfClosed();
    if (!getServerMetaSupported()) {
      return super.getMaxStatementLength();
    }
    return getServerMeta().getMaxStatementLength();
  }

  @Override
  public int getMaxStatements() throws SQLException {
    throwIfClosed();
    if (!getServerMetaSupported()) {
      return super.getMaxStatements();
    }
    return getServerMeta().getMaxStatements();
  }

  @Override
  public int getMaxTableNameLength() throws SQLException {
    throwIfClosed();
    if (!getServerMetaSupported()) {
      return super.getMaxTableNameLength();
    }
    return getServerMeta().getMaxTableNameLength();
  }

  @Override
  public int getMaxTablesInSelect() throws SQLException {
    throwIfClosed();
    if (!getServerMetaSupported()) {
      return super.getMaxTablesInSelect();
    }
    return getServerMeta().getMaxTablesInSelect();
  }

  @Override
  public int getMaxUserNameLength() throws SQLException {
    throwIfClosed();
    if (!getServerMetaSupported()) {
      return super.getMaxUserNameLength();
    }
    return getServerMeta().getMaxUserNameLength();
  }

  @Override
  public int getDefaultTransactionIsolation() throws SQLException {
    throwIfClosed();
    return super.getDefaultTransactionIsolation();
  }

  @Override
  public boolean supportsTransactions() throws SQLException {
    throwIfClosed();
    if (!getServerMetaSupported()) {
      return super.supportsTransactions();
    }
    return getServerMeta().getTransactionSupported();
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
    try {
      return super.getTables(catalog, schemaPattern,tableNamePattern, types);
    } catch(DrillRuntimeException e) {
      Throwables.propagateIfInstanceOf(e.getCause(), SQLException.class);
      throw e;
    }
  }


  @Override
  public ResultSet getSchemas() throws SQLException {
    throwIfClosed();
    try {
      return super.getSchemas();
    } catch(DrillRuntimeException e) {
      Throwables.propagateIfInstanceOf(e.getCause(), SQLException.class);
      throw e;
    }
  }

  @Override
  public ResultSet getCatalogs() throws SQLException {
    throwIfClosed();
    try {
      return super.getCatalogs();
    } catch(DrillRuntimeException e) {
      Throwables.propagateIfInstanceOf(e.getCause(), SQLException.class);
      throw e;
    }
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
    try {
      return super.getColumns(catalog, schema, table, columnNamePattern);
    } catch(DrillRuntimeException e) {
      Throwables.propagateIfInstanceOf(e.getCause(), SQLException.class);
      throw e;
    }
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
    Version version = getServerVersion();
    if (version == null) {
      return super.getDatabaseMajorVersion();
    }
    return version.getMajorVersion();
  }

  @Override
  public int getDatabaseMinorVersion() throws SQLException {
    throwIfClosed();
    Version version = getServerVersion();
    if (version == null) {
      return super.getDatabaseMinorVersion();
    }
    return version.getMinorVersion();
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
