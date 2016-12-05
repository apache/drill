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

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.Executor;

import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.AvaticaFactory;
import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.Meta.ExecuteResult;
import org.apache.calcite.avatica.UnregisteredDriver;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.memory.RootAllocatorFactory;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.InvalidConnectionInfoException;
import org.apache.drill.exec.server.Drillbit;
import org.apache.drill.exec.server.RemoteServiceSet;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.util.TestUtilities;
import org.apache.drill.jdbc.AlreadyClosedSqlException;
import org.apache.drill.jdbc.DrillConnection;
import org.apache.drill.jdbc.DrillConnectionConfig;
import org.apache.drill.jdbc.InvalidParameterSqlException;
import org.apache.drill.jdbc.JdbcApiSqlException;
import org.slf4j.Logger;

import com.google.common.base.Throwables;

/**
 * Drill's implementation of {@link Connection}.
 */
// (Was abstract to avoid errors _here_ if newer versions of JDBC added
// interface methods, but now newer versions would probably use Java 8's default
// methods for compatibility.)
class DrillConnectionImpl extends AvaticaConnection
                          implements DrillConnection {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(DrillConnection.class);

  final DrillStatementRegistry openStatementsRegistry = new DrillStatementRegistry();
  final DrillConnectionConfig config;

  private final DrillClient client;
  private final BufferAllocator allocator;
  private Drillbit bit;
  private RemoteServiceSet serviceSet;


  protected DrillConnectionImpl(DriverImpl driver, AvaticaFactory factory,
                                String url, Properties info) throws SQLException {
    super(driver, factory, url, info);

    // Initialize transaction-related settings per Drill behavior.
    super.setTransactionIsolation( TRANSACTION_NONE );
    super.setAutoCommit( true );

    this.config = new DrillConnectionConfig(info);

    try {
      String connect = null;

      if (config.isLocal()) {
        try {
          Class.forName("org.eclipse.jetty.server.Handler");
        } catch (final ClassNotFoundException e) {
          throw new SQLNonTransientConnectionException(
              "Running Drill in embedded mode using Drill's jdbc-all JDBC"
              + " driver Jar file alone is not supported.",  e);
        }

        final DrillConfig dConfig = DrillConfig.create(info);
        this.allocator = RootAllocatorFactory.newRoot(dConfig);
        RemoteServiceSet set = GlobalServiceSetReference.SETS.get();
        if (set == null) {
          // We're embedded; start a local drill bit.
          serviceSet = RemoteServiceSet.getLocalServiceSet();
          set = serviceSet;
          try {
            bit = new Drillbit(dConfig, serviceSet);
            bit.run();
          } catch (final UserException e) {
            throw new SQLException(
                "Failure in starting embedded Drillbit: " + e.getMessage(),
                e);
          } catch (Exception e) {
            // (Include cause exception's text in wrapping exception's text so
            // it's more likely to get to user (e.g., via SQLLine), and use
            // toString() since getMessage() text doesn't always mention error:)
            throw new SQLException("Failure in starting embedded Drillbit: " + e, e);
          }
        } else {
          serviceSet = null;
          bit = null;
        }

        makeTmpSchemaLocationsUnique(bit.getContext().getStorage(), info);

        this.client = new DrillClient(dConfig, set.getCoordinator());
      } else if(config.isDirect()) {
        final DrillConfig dConfig = DrillConfig.forClient();
        this.allocator = RootAllocatorFactory.newRoot(dConfig);
        this.client = new DrillClient(dConfig, true); // Get a direct connection
        connect = config.getZookeeperConnectionString();
      } else {
        final DrillConfig dConfig = DrillConfig.forClient();
        this.allocator = RootAllocatorFactory.newRoot(dConfig);
        // TODO:  Check:  Why does new DrillClient() create another DrillConfig,
        // with enableServerConfigs true, and cause scanning for function
        // implementations (needed by a server, but not by a client-only
        // process, right?)?  Probably pass dConfig to construction.
        this.client = new DrillClient();
        connect = config.getZookeeperConnectionString();
      }
      this.client.setClientName("Apache Drill JDBC Driver");
      this.client.connect(connect, info);
    } catch (OutOfMemoryException e) {
      throw new SQLException("Failure creating root allocator", e);
    } catch (InvalidConnectionInfoException e) {
      throw new SQLException("Invalid parameter in connection string: " + e.getMessage(), e);
    } catch (RpcException e) {
      // (Include cause exception's text in wrapping exception's text so
      // it's more likely to get to user (e.g., via SQLLine), and use
      // toString() since getMessage() text doesn't always mention error:)
      throw new SQLException("Failure in connecting to Drill: " + e, e);
    }
  }

  @Override
  protected ExecuteResult prepareAndExecuteInternal(AvaticaStatement statement, String sql, long maxRowCount)
      throws SQLException {
    try {
      return super.prepareAndExecuteInternal(statement, sql, maxRowCount);
    } catch(RuntimeException e) {
      Throwables.propagateIfInstanceOf(e.getCause(), SQLException.class);
      throw e;
    }
  }
  /**
   * Throws AlreadyClosedSqlException <i>iff</i> this Connection is closed.
   *
   * @throws  AlreadyClosedSqlException  if Connection is closed
   */
  private void throwIfClosed() throws AlreadyClosedSqlException {
    if ( isClosed() ) {
      throw new AlreadyClosedSqlException( "Connection is already closed." );
    }
  }

  @Override
  public DrillConnectionConfig getConfig() {
    return config;
  }

  BufferAllocator getAllocator() {
    return allocator;
  }

  @Override
  public DrillClient getClient() {
    return client;
  }

  @Override
  public void setAutoCommit( boolean autoCommit ) throws SQLException {
    throwIfClosed();
    if ( ! autoCommit ) {
      throw new SQLFeatureNotSupportedException(
          "Can't turn off auto-committing; transactions are not supported.  "
          + "(Drill is not transactional.)" );
    }
    assert getAutoCommit() : "getAutoCommit() = " + getAutoCommit();
  }

  @Override
  public void commit() throws SQLException {
    throwIfClosed();
    if ( getAutoCommit() ) {
      throw new JdbcApiSqlException( "Can't call commit() in auto-commit mode." );
    }
    else {
      // (Currently not reachable.)
      throw new SQLFeatureNotSupportedException(
          "Connection.commit() is not supported.  (Drill is not transactional.)" );
    }
  }

  @Override
  public void rollback() throws SQLException {
    throwIfClosed();
    if ( getAutoCommit()  ) {
      throw new JdbcApiSqlException( "Can't call rollback() in auto-commit mode." );
    }
    else {
      // (Currently not reachable.)
      throw new SQLFeatureNotSupportedException(
          "Connection.rollback() is not supported.  (Drill is not transactional.)" );
    }
  }


  @Override
  public boolean isClosed() {
    try {
      return super.isClosed();
    }
    catch ( SQLException e ) {
      // Currently can't happen, since AvaticaConnection.isClosed() never throws
      // SQLException.
      throw new DrillRuntimeException(
          "Unexpected exception from " + getClass().getSuperclass()
          + ".isClosed(): " + e,
          e );
    }
  }


  @Override
  public Savepoint setSavepoint() throws SQLException {
    throwIfClosed();
    throw new SQLFeatureNotSupportedException(
        "Savepoints are not supported.  (Drill is not transactional.)" );
  }

  @Override
  public Savepoint setSavepoint(String name) throws SQLException {
    throwIfClosed();
    throw new SQLFeatureNotSupportedException(
        "Savepoints are not supported.  (Drill is not transactional.)" );
  }

  @Override
    public void rollback(Savepoint savepoint) throws SQLException {
    throwIfClosed();
    throw new SQLFeatureNotSupportedException(
        "Savepoints are not supported.  (Drill is not transactional.)" );
  }

  @Override
  public void releaseSavepoint(Savepoint savepoint) throws SQLException {
    throwIfClosed();
    throw new SQLFeatureNotSupportedException(
        "Savepoints are not supported.  (Drill is not transactional.)" );
  }


  private String isolationValueToString( final int level ) {
    switch ( level ) {
      case TRANSACTION_NONE:             return "TRANSACTION_NONE";
      case TRANSACTION_READ_UNCOMMITTED: return "TRANSACTION_READ_UNCOMMITTED";
      case TRANSACTION_READ_COMMITTED:   return "TRANSACTION_READ_COMMITTED";
      case TRANSACTION_REPEATABLE_READ:  return "TRANSACTION_REPEATABLE_READ";
      case TRANSACTION_SERIALIZABLE:     return "TRANSACTION_SERIALIZABLE";
      default:
        return "<Unknown transaction isolation level value " + level + ">";
    }
  }

  @Override
  public void setTransactionIsolation(int level) throws SQLException {
    throwIfClosed();
    switch ( level ) {
      case TRANSACTION_NONE:
        // No-op.  (Is already set in constructor, and we disallow changing it.)
        break;
      case TRANSACTION_READ_UNCOMMITTED:
      case TRANSACTION_READ_COMMITTED:
      case TRANSACTION_REPEATABLE_READ:
      case TRANSACTION_SERIALIZABLE:
          throw new SQLFeatureNotSupportedException(
              "Can't change transaction isolation level to Connection."
              + isolationValueToString( level ) + " (from Connection."
              + isolationValueToString( getTransactionIsolation() ) + ")."
              + "  (Drill is not transactional.)" );
      default:
        // Invalid value (or new one unknown to code).
        throw new JdbcApiSqlException(
            "Invalid transaction isolation level value " + level );
        //break;
    }
  }

  @Override
  public void setNetworkTimeout( Executor executor, int milliseconds )
      throws AlreadyClosedSqlException,
             JdbcApiSqlException,
             SQLFeatureNotSupportedException {
    throwIfClosed();
    if ( null == executor ) {
      throw new InvalidParameterSqlException(
          "Invalid (null) \"executor\" parameter to setNetworkTimeout(...)" );
    }
    else if ( milliseconds < 0 ) {
      throw new InvalidParameterSqlException(
          "Invalid (negative) \"milliseconds\" parameter to"
          + " setNetworkTimeout(...) (" + milliseconds + ")" );
    }
    else {
      if ( 0 != milliseconds ) {
        throw new SQLFeatureNotSupportedException(
            "Setting network timeout is not supported." );
      }
    }
  }

  @Override
  public int getNetworkTimeout() throws AlreadyClosedSqlException
  {
    throwIfClosed();
    return 0;  // (No timeout.)
  }


  @Override
  public DrillStatementImpl createStatement(int resultSetType,
                                            int resultSetConcurrency,
                                            int resultSetHoldability) throws SQLException {
    throwIfClosed();
    DrillStatementImpl statement =
        (DrillStatementImpl) super.createStatement(resultSetType,
                                                   resultSetConcurrency,
                                                   resultSetHoldability);
    return statement;
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int resultSetType,
                                            int resultSetConcurrency,
                                            int resultSetHoldability) throws SQLException {
    throwIfClosed();
    DrillPreparedStatementImpl statement =
        (DrillPreparedStatementImpl) super.prepareStatement(sql,
                                                            resultSetType,
                                                            resultSetConcurrency,
                                                            resultSetHoldability);
    return statement;
  }

  @Override
  public TimeZone getTimeZone() {
    return config.getTimeZone();
  }


  // Note:  Using dynamic proxies would reduce the quantity (450?) of method
  // overrides by eliminating those that exist solely to check whether the
  // object is closed.  It would also eliminate the need to throw non-compliant
  // RuntimeExceptions when Avatica's method declarations won't let us throw
  // proper SQLExceptions. (Check performance before applying to frequently
  // called ResultSet.)

  // No isWrapperFor(Class<?>) (it doesn't throw SQLException if already closed).
  // No unwrap(Class<T>) (it doesn't throw SQLException if already closed).

  @Override
  public AvaticaStatement createStatement() throws SQLException {
    throwIfClosed();
    return super.createStatement();
  }

  @Override
  public PreparedStatement prepareStatement(String sql) throws SQLException {
    throwIfClosed();
    return super.prepareStatement(sql);
  }

  @Override
  public CallableStatement prepareCall(String sql) throws SQLException {
    throwIfClosed();
    try {
      return super.prepareCall(sql);
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public String nativeSQL(String sql) throws SQLException {
    throwIfClosed();
    try {
      return super.nativeSQL(sql);
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }


  @Override
  public boolean getAutoCommit() throws SQLException {
    throwIfClosed();
    return super.getAutoCommit();
  }

  // No close() (it doesn't throw SQLException if already closed).

  @Override
  public DatabaseMetaData getMetaData() throws SQLException {
    throwIfClosed();
    return super.getMetaData();
  }

  @Override
  public void setReadOnly(boolean readOnly) throws SQLException {
    throwIfClosed();
    super.setReadOnly(readOnly);
  }

  @Override
  public boolean isReadOnly() throws SQLException {
    throwIfClosed();
    return super.isReadOnly();
  }

  @Override
  public void setCatalog(String catalog) throws SQLException {
    throwIfClosed();
    super.setCatalog(catalog);
  }

  @Override
  public String getCatalog() {
    // Can't throw any SQLException because AvaticaConnection's getCatalog() is
    // missing "throws SQLException".
    try {
      throwIfClosed();
    } catch (AlreadyClosedSqlException e) {
      throw new RuntimeException(e.getMessage(), e);
    }
    return super.getCatalog();
  }

  @Override
  public int getTransactionIsolation() throws SQLException {
    throwIfClosed();
    return super.getTransactionIsolation();
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    throwIfClosed();
    return super.getWarnings();
  }

  @Override
  public void clearWarnings() throws SQLException {
    throwIfClosed();
    super.clearWarnings();
  }

  @Override
  public Statement createStatement(int resultSetType,
                                   int resultSetConcurrency) throws SQLException {
    throwIfClosed();
    return super.createStatement(resultSetType, resultSetConcurrency);
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int resultSetType,
                                            int resultSetConcurrency) throws SQLException {
    throwIfClosed();
    return super.prepareStatement(sql, resultSetType, resultSetConcurrency);
  }

  @Override
  public CallableStatement prepareCall(String sql, int resultSetType,
                                       int resultSetConcurrency) throws SQLException {
    throwIfClosed();
    try {
      return super.prepareCall(sql, resultSetType, resultSetConcurrency);
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public Map<String,Class<?>> getTypeMap() throws SQLException {
    throwIfClosed();
    try {
      return super.getTypeMap();
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public void setTypeMap(Map<String,Class<?>> map) throws SQLException {
    throwIfClosed();
    try {
      super.setTypeMap(map);
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public void setHoldability(int holdability) throws SQLException {
    throwIfClosed();
    super.setHoldability(holdability);
  }

  @Override
  public int getHoldability() throws SQLException {
    throwIfClosed();
    return super.getHoldability();
  }

  @Override
  public CallableStatement prepareCall(String sql, int resultSetType,
                                       int resultSetConcurrency,
                                       int resultSetHoldability) throws SQLException {
    throwIfClosed();
    try {
      return super.prepareCall(sql, resultSetType, resultSetConcurrency,
                               resultSetHoldability);
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public PreparedStatement prepareStatement(String sql,
                                            int autoGeneratedKeys) throws SQLException {
    throwIfClosed();
    try {
      return super.prepareStatement(sql, autoGeneratedKeys);
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public PreparedStatement prepareStatement(String sql,
                                            int columnIndexes[]) throws SQLException {
    throwIfClosed();
    try {
      return super.prepareStatement(sql, columnIndexes);
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public PreparedStatement prepareStatement(String sql,
                                            String columnNames[]) throws SQLException {
    throwIfClosed();
    try {
      return super.prepareStatement(sql, columnNames);
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public Clob createClob() throws SQLException {
    throwIfClosed();
    try {
      return super.createClob();
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public Blob createBlob() throws SQLException {
    throwIfClosed();
    try {
      return super.createBlob();
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public NClob createNClob() throws SQLException {
    throwIfClosed();
    try {
      return super.createNClob();
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public SQLXML createSQLXML() throws SQLException {
    throwIfClosed();
    try {
      return super.createSQLXML();
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public boolean isValid(int timeout) throws SQLException {
    throwIfClosed();
    try {
      return super.isValid(timeout);
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public void setClientInfo(String name, String value) throws SQLClientInfoException {
    try {
      throwIfClosed();
    } catch (AlreadyClosedSqlException e) {
      throw new SQLClientInfoException(e.getMessage(), null, e);
    }
    try {
      super.setClientInfo(name,  value);
    }
    catch (UnsupportedOperationException e) {
      SQLFeatureNotSupportedException intended =
          new SQLFeatureNotSupportedException(e.getMessage(), e);
      throw new SQLClientInfoException(e.getMessage(), null, intended);
    }
  }

  @Override
  public void setClientInfo(Properties properties) throws SQLClientInfoException {
    try {
      throwIfClosed();
    } catch (AlreadyClosedSqlException e) {
      throw new SQLClientInfoException(e.getMessage(), null, e);
    }
    try {
      super.setClientInfo(properties);
    }
    catch (UnsupportedOperationException e) {
      SQLFeatureNotSupportedException intended =
          new SQLFeatureNotSupportedException(e.getMessage(), e);
      throw new SQLClientInfoException(e.getMessage(), null, intended);
    }
  }

  @Override
  public String getClientInfo(String name) throws SQLException {
    throwIfClosed();
    try {
      return super.getClientInfo(name);
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public Properties getClientInfo() throws SQLException {
    throwIfClosed();
    try {
      return super.getClientInfo();
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
    throwIfClosed();
    try {
      return super.createArrayOf(typeName, elements);
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
    throwIfClosed();
    try {
      return super.createStruct(typeName, attributes);
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public void setSchema(String schema) throws SQLException {
    throwIfClosed();
    super.setSchema(schema);
  }

  @Override
  public String getSchema() {
    // Can't throw any SQLException because AvaticaConnection's getCatalog() is
    // missing "throws SQLException".
    try {
      throwIfClosed();
    } catch (AlreadyClosedSqlException e) {
      throw new RuntimeException(e.getMessage(), e);
    }
    return super.getSchema();
  }

  @Override
  public void abort(Executor executor) throws SQLException {
    throwIfClosed();
    try {
      super.abort(executor);
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }



  // do not make public
  UnregisteredDriver getDriver() {
    return driver;
  }

  // do not make public
  AvaticaFactory getFactory() {
    return factory;
  }

  private static void closeOrWarn(final AutoCloseable autoCloseable, final String message, final Logger logger) {
    if (autoCloseable == null) {
      return;
    }

    try {
      autoCloseable.close();
    } catch(Exception e) {
      logger.warn(message, e);
    }
  }

  // TODO this should be an AutoCloseable, and this should be close()
  void cleanup() {
    // First close any open JDBC Statement objects, to close any open ResultSet
    // objects and release their buffers/vectors.
    openStatementsRegistry.close();

    // TODO all of these should use DeferredException when it is available from DRILL-2245
    closeOrWarn(client, "Exception while closing client.", logger);
    closeOrWarn(allocator, "Exception while closing allocator.", logger);

    if (bit != null) {
      bit.close();
    }

    closeOrWarn(serviceSet, "Exception while closing service set.", logger);
  }

  // TODO(DRILL-xxxx):  Eliminate this test-specific hack from production code.
  // If we're not going to have tests themselves explicitly handle making names
  // unique, then at least move this logic into a test base class, and have it
  // go through DrillConnection.getClient().
  /**
   * Test only code to make JDBC tests run concurrently. If the property <i>drillJDBCUnitTests</i> is set to
   * <i>true</i> in connection properties:
   *   - Update dfs_test.tmp workspace location with a temp directory. This temp is for exclusive use for test jvm.
   *   - Update dfs.tmp workspace to immutable, so that test writer don't try to create views in dfs.tmp
   * @param pluginRegistry
   */
  private static void makeTmpSchemaLocationsUnique(StoragePluginRegistry pluginRegistry, Properties props) {
    try {
      if (props != null && "true".equalsIgnoreCase(props.getProperty("drillJDBCUnitTests"))) {
        final String tmpDirPath = TestUtilities.createTempDir();
        TestUtilities.updateDfsTestTmpSchemaLocation(pluginRegistry, tmpDirPath);
        TestUtilities.makeDfsTmpSchemaImmutable(pluginRegistry);
      }
    } catch(Throwable e) {
      // Reason for catching Throwable is to capture NoSuchMethodError etc which depend on certain classed to be
      // present in classpath which may not be available when just using the standalone JDBC. This is unlikely to
      // happen, but just a safeguard to avoid failing user applications.
      logger.warn("Failed to update tmp schema locations. This step is purely for testing purpose. " +
          "Shouldn't be seen in production code.");
      // Ignore the error and go with defaults
    }
  }
}
