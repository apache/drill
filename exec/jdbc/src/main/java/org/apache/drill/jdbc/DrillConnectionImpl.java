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

import net.hydromatic.avatica.AvaticaConnection;
import net.hydromatic.avatica.AvaticaFactory;
import net.hydromatic.avatica.Helper;
import net.hydromatic.avatica.Meta;
import net.hydromatic.avatica.UnregisteredDriver;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.memory.TopLevelAllocator;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.server.Drillbit;
import org.apache.drill.exec.server.RemoteServiceSet;

/**
 * Implementation of JDBC connection in Drill.
 *
 * <p>
 * Abstract to allow newer versions of JDBC to add methods.
 * </p>
 */
abstract class DrillConnectionImpl extends AvaticaConnection implements org.apache.drill.jdbc.DrillConnection {
  public final DrillStatementRegistry registry = new DrillStatementRegistry();
  final DrillConnectionConfig config;

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillConnection.class);

  private final DrillClient client;
  private final BufferAllocator allocator;
  private Drillbit bit;
  private RemoteServiceSet serviceSet;

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
          // we're embedded, start a local drill bit.
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
        this.client = new DrillClient(dConfig, set.getCoordinator());
        this.client.connect(null, info);
      } else {
        final DrillConfig dConfig = DrillConfig.forClient();
        this.allocator = new TopLevelAllocator(dConfig);
        this.client = new DrillClient();
        this.client.connect(config.getZookeeperConnectionString(), info);
      }
    } catch (RpcException e) {
      throw new SQLException("Failure while attempting to connect to Drill.", e);
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

  public DrillClient getClient() {
    return client;
  }

  @Override
  public DrillStatement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
      throws SQLException {
    DrillStatement statement = (DrillStatement) super.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);
    registry.addStatement(statement);
    return statement;
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
      int resultSetHoldability) throws SQLException {
    try {
      DrillPrepareResult prepareResult = new DrillPrepareResult(sql);
      DrillPreparedStatement statement = (DrillPreparedStatement) factory.newPreparedStatement(this, prepareResult,
          resultSetType, resultSetConcurrency, resultSetHoldability);
      registry.addStatement(statement);
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

  void cleanup() {
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

}
