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

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.Savepoint;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.Executor;

import net.hydromatic.avatica.AvaticaConnection;
import net.hydromatic.avatica.AvaticaFactory;
import net.hydromatic.avatica.Helper;
import net.hydromatic.avatica.Meta;
import net.hydromatic.avatica.UnregisteredDriver;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.memory.OutOfMemoryException;
import org.apache.drill.exec.memory.RootAllocatorFactory;
import org.apache.drill.exec.rpc.RpcException;
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

/**
 * Drill's implementation of {@link Connection}.
 */
// (Was abstract to avoid errors _here_ if newer versions of JDBC added
// interface methods, but now newer versions would probably use Java 8's default
// methods for compatibility.)
class DrillConnectionImpl extends AvaticaConnection
                                   implements DrillConnection {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillConnection.class);

  final DrillStatementRegistry openStatementsRegistry = new DrillStatementRegistry();
  final DrillConnectionConfig config;

  private final DrillClient client;
  private final BufferAllocator allocator;
  private Drillbit bit;
  private RemoteServiceSet serviceSet;

  protected DrillConnectionImpl(DriverImpl driver, AvaticaFactory factory, String url, Properties info) throws SQLException {
    super(driver, factory, url, info);

    // Initialize transaction-related settings per Drill behavior.
    super.setTransactionIsolation( TRANSACTION_NONE );
    super.setAutoCommit( true );

    this.config = new DrillConnectionConfig(info);

    try {
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
        this.client.connect(null, info);
      } else if(config.isDirect()) {
        final DrillConfig dConfig = DrillConfig.forClient();
        this.allocator = RootAllocatorFactory.newRoot(dConfig);
        this.client = new DrillClient(dConfig, true); // Get a direct connection
        this.client.connect(config.getZookeeperConnectionString(), info);
      } else {
        final DrillConfig dConfig = DrillConfig.forClient();
        this.allocator = RootAllocatorFactory.newRoot(dConfig);
        // TODO:  Check:  Why does new DrillClient() create another DrillConfig,
        // with enableServerConfigs true, and cause scanning for function
        // implementations (needed by a server, but not by a client-only
        // process, right?)?  Probably pass dConfig to construction.
        this.client = new DrillClient();
        this.client.connect(config.getZookeeperConnectionString(), info);
      }
    } catch (OutOfMemoryException e) {
      throw new SQLException("Failure creating root allocator", e);
    } catch (RpcException e) {
      // (Include cause exception's text in wrapping exception's text so
      // it's more likely to get to user (e.g., via SQLLine), and use
      // toString() since getMessage() text doesn't always mention error:)
      throw new SQLException("Failure in connecting to Drill: " + e, e);
    }
  }

  /**
   * Throws AlreadyClosedSqlException <i>iff</i> this Connection is closed.
   *
   * @throws  AlreadyClosedSqlException  if Connection is closed   */
  private void checkNotClosed() throws AlreadyClosedSqlException {
    if ( isClosed() ) {
      throw new AlreadyClosedSqlException( "Connection is already closed." );
    }
  }

  @Override
  public DrillConnectionConfig getConfig() {
    return config;
  }

  @Override
  protected Meta createMeta() {
    return new MetaImpl(this);
  }

  MetaImpl meta() {
    return (MetaImpl) meta;
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
    checkNotClosed();
    if ( ! autoCommit ) {
      throw new SQLFeatureNotSupportedException(
          "Can't turn off auto-committing; transactions are not supported.  "
          + "(Drill is not transactional.)" );
    }
    assert getAutoCommit() : "getAutoCommit() = " + getAutoCommit();
  }

  @Override
  public void commit() throws SQLException {
    checkNotClosed();
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
    checkNotClosed();
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
    checkNotClosed();
    throw new SQLFeatureNotSupportedException(
        "Savepoints are not supported.  (Drill is not transactional.)" );
  }

  @Override
  public Savepoint setSavepoint(String name) throws SQLException {
    checkNotClosed();
    throw new SQLFeatureNotSupportedException(
        "Savepoints are not supported.  (Drill is not transactional.)" );
  }

  @Override
    public void rollback(Savepoint savepoint) throws SQLException {
    checkNotClosed();
    throw new SQLFeatureNotSupportedException(
        "Savepoints are not supported.  (Drill is not transactional.)" );
  }

  @Override
  public void releaseSavepoint(Savepoint savepoint) throws SQLException {
    checkNotClosed();
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
    checkNotClosed();
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
    checkNotClosed();
    if ( null == executor ) {
      throw new InvalidParameterSqlException(
          "Invalid (null) \"executor\" parameter to setNetworkTimeout(...)" );
    }
    else if ( milliseconds < 0 ) {
      throw new InvalidParameterSqlException(
          "Invalid (negative) \"milliseconds\" parameter to setNetworkTimeout(...)"
          + " (" + milliseconds + ")" );
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
    checkNotClosed();
    return 0;  // (No no timeout.)
  }


  @Override
  public DrillStatementImpl createStatement(int resultSetType, int resultSetConcurrency,
                                            int resultSetHoldability) throws SQLException {
    checkNotClosed();
    DrillStatementImpl statement =
        (DrillStatementImpl) super.createStatement(resultSetType, resultSetConcurrency,
                                                   resultSetHoldability);
    return statement;
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int resultSetType,
                                            int resultSetConcurrency,
                                            int resultSetHoldability) throws SQLException {
    checkNotClosed();
    try {
      DrillPrepareResult prepareResult = new DrillPrepareResult(sql);
      DrillPreparedStatementImpl statement =
          (DrillPreparedStatementImpl) factory.newPreparedStatement(
              this, prepareResult, resultSetType, resultSetConcurrency,
              resultSetHoldability);
      return statement;
    } catch (RuntimeException e) {
      throw Helper.INSTANCE.createException("Error while preparing statement [" + sql + "]", e);
    } catch (Exception e) {
      throw Helper.INSTANCE.createException("Error while preparing statement [" + sql + "]", e);
    }
  }

  @Override
  public TimeZone getTimeZone() {
    return config.getTimeZone();
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
