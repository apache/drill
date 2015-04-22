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

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;
import java.util.TimeZone;

import com.google.common.io.Files;
import net.hydromatic.avatica.AvaticaConnection;
import net.hydromatic.avatica.AvaticaFactory;
import net.hydromatic.avatica.Helper;
import net.hydromatic.avatica.Meta;
import net.hydromatic.avatica.UnregisteredDriver;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.memory.TopLevelAllocator;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.server.Drillbit;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.server.RemoteServiceSet;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.dfs.FileSystemConfig;
import org.apache.drill.exec.store.dfs.FileSystemPlugin;
import org.apache.drill.exec.store.dfs.WorkspaceConfig;
import org.apache.drill.exec.util.TestUtilities;

// (Public until JDBC impl. classes moved out of published-intf. package. (DRILL-2089).)
/**
 * Implementation of JDBC connection in Drill.
 *
 * <p>
 * Abstract to allow newer versions of JDBC to add methods.
 * </p>
 */
public abstract class DrillConnectionImpl extends AvaticaConnection implements DrillConnection {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillConnection.class);

  final DrillStatementRegistry openStatementsRegistry = new DrillStatementRegistry();
  final DrillConnectionConfig config;

  private final DrillClient client;
  private final BufferAllocator allocator;
  private Drillbit bit;
  private RemoteServiceSet serviceSet;

  /**
   * Throws AlreadyClosedSqlException if this Connection is closed.
   *
   * @throws AlreadyClosedSqlException if Connection is closed
   * @throws SQLException if error in calling {@link #isClosed()}
   */
  private void checkNotClosed() throws SQLException {
    if ( isClosed() ) {
      throw new AlreadyClosedSqlException( "Connection is already closed." );
    }
  }

  protected DrillConnectionImpl(Driver driver, AvaticaFactory factory, String url, Properties info) throws SQLException {
    super(driver, factory, url, info);
    this.config = new DrillConnectionConfig(info);

    try {
      if (config.isLocal()) {
        try {
          Class.forName("org.eclipse.jetty.server.Handler");
        } catch (ClassNotFoundException e1) {
          throw new SQLException("Running Drill in embedded mode using the JDBC jar alone is not supported.");
        }

        final DrillConfig dConfig = DrillConfig.create(info);
        this.allocator = new TopLevelAllocator(dConfig);
        RemoteServiceSet set = GlobalServiceSetReference.SETS.get();
        if (set == null) {
          // We're embedded; start a local drill bit.
          serviceSet = RemoteServiceSet.getLocalServiceSet();
          set = serviceSet;
          try {
            bit = new Drillbit(dConfig, serviceSet);
            bit.run();
          } catch (Exception e) {
            throw new SQLException("Failure while attempting to start Drillbit in embedded mode.", e);
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
        this.allocator = new TopLevelAllocator(dConfig);
        this.client = new DrillClient(true); // Get a direct connection
        this.client.connect(config.getZookeeperConnectionString(), info);
      } else {
        final DrillConfig dConfig = DrillConfig.forClient();
        this.allocator = new TopLevelAllocator(dConfig);
        // TODO:  Check:  Why does new DrillClient() create another DrillConfig,
        // with enableServerConfigs true, and cause scanning for function
        // implementations (needed by a server, but not by a client-only
        // process, right?)?  Probably pass dConfig to construction.
        this.client = new DrillClient();
        this.client.connect(config.getZookeeperConnectionString(), info);
      }
    } catch (RpcException e) {
      throw new SQLException("Failure while attempting to connect to Drill: " + e.getMessage(), e);
    }
  }

  @Override
  public DrillConnectionConfig config() {
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
  public DrillStatement createStatement(int resultSetType, int resultSetConcurrency,
                                        int resultSetHoldability) throws SQLException {
    checkNotClosed();
    DrillStatement statement =
        (DrillStatement) super.createStatement(resultSetType, resultSetConcurrency,
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
      DrillPreparedStatement statement =
          (DrillPreparedStatement) factory.newPreparedStatement(
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

  // (Public until JDBC impl. classes moved out of published-intf. package. (DRILL-2089).)
  // do not make public
  public UnregisteredDriver getDriver() {
    return driver;
  }

  // do not make public
  AvaticaFactory getFactory() {
    return factory;
  }

  void cleanup() {
    // First close any open JDBC Statement objects, to close any open ResultSet
    // objects and release their buffers/vectors.
    openStatementsRegistry.close();

    client.close();
    allocator.close();
    if (bit != null) {
      bit.close();
    }

    if (serviceSet != null) {
      try {
        serviceSet.close();
      } catch (IOException e) {
        logger.warn("Exception while closing service set.", e);
      }
    }
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
        TestUtilities.updateDfsTestTmpSchemaLocation(pluginRegistry);
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
