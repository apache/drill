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
package org.apache.drill.exec.store.phoenix;

import java.io.PrintWriter;
import java.security.PrivilegedExceptionAction;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Supplier;
import java.util.logging.Logger;

import javax.sql.DataSource;

import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.util.function.CheckedSupplier;
import org.apache.drill.exec.util.ImpersonationUtil;
import com.google.common.base.Preconditions;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.LoggerFactory;

/**
 * Phoenix’s Connection objects are different from most other JDBC Connections
 * due to the underlying HBase connection. The Phoenix Connection object
 * is designed to be a thin object that is inexpensive to create.
 * <p>
 * If Phoenix Connections are reused, it is possible that the underlying HBase connection
 * is not always left in a healthy state by the previous user. It is better to
 * create new Phoenix Connections to ensure that you avoid any potential issues.
 */
public class PhoenixDataSource implements DataSource {
  private static final org.slf4j.Logger logger = LoggerFactory.getLogger(PhoenixDataSource.class);
  private static final String DEFAULT_URL_HEADER = "jdbc:phoenix:";

  private final String url;
  private final String user;
  private final Map<String, Object> connectionProperties;
  private final boolean impersonationEnabled;

  public PhoenixDataSource(String url,
                           String userName,
                           Map<String, Object> connectionProperties,
                           boolean impersonationEnabled) {
    Preconditions.checkNotNull(url, userName);
    Preconditions.checkNotNull(connectionProperties);
    connectionProperties.forEach((k, v)
        -> Preconditions.checkArgument(v != null, String.format("does not accept null values : %s", k)));
    this.impersonationEnabled = impersonationEnabled;
    this.url = url;
    this.user = userName;
    this.connectionProperties = connectionProperties;
  }

  public PhoenixDataSource(String zkQuorum,
                           int port,
                           String zkPath,
                           String userName,
                           Map<String, Object> connectionProperties,
                           boolean impersonationEnabled) {
    Preconditions.checkNotNull(zkQuorum, userName);
    connectionProperties.forEach((k, v)
      -> Preconditions.checkArgument(v != null, String.format("does not accept null values : %s", k)));
    StringBuilder stringBuilder = new StringBuilder()
      .append(DEFAULT_URL_HEADER)
      .append(zkQuorum)
      .append(":")
      .append(port);
    Optional.ofNullable(zkPath)
      .ifPresent(path -> stringBuilder.append(":").append(path));
    this.url = stringBuilder.toString();
    this.impersonationEnabled = impersonationEnabled;
    this.user = userName;
    this.connectionProperties = connectionProperties;
  }

  public Map<String, Object> getConnectionProperties() {
    return connectionProperties;
  }

  @Override
  public PrintWriter getLogWriter() {
    throw new UnsupportedOperationException("getLogWriter");
  }

  @Override
  public void setLogWriter(PrintWriter out) {
    throw new UnsupportedOperationException("setLogWriter");
  }

  @Override
  public void setLoginTimeout(int seconds) {
    throw new UnsupportedOperationException("setLoginTimeout");
  }

  @Override
  public int getLoginTimeout() {
    return 0;
  }

  @Override
  public Logger getParentLogger() {
    throw new UnsupportedOperationException("getParentLogger");
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> T unwrap(Class<T> iface) throws SQLException {
    if (iface.isInstance(this)) {
      return (T) this;
    }
    throw new SQLException("DataSource of type [" + getClass().getName() +
        "] cannot be unwrapped as [" + iface.getName() + "]");
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) {
    return iface.isInstance(this);
  }

  @Override
  public Connection getConnection() throws SQLException {
    loadDriverClass();
    return getConnection(this.user, null);
  }

  @Override
  public Connection getConnection(String userName, String password) throws SQLException {
    loadDriverClass();
    logger.debug("Drill/Phoenix connection url: {}", url);
    CheckedSupplier<Connection, SQLException> action =
      () -> DriverManager.getConnection(url, useConfProperties());
    if (impersonationEnabled) {
      return doAsRemoteUser(userName, action);
    }
    return action.getAndThrow();
  }

  private <T> T doAsRemoteUser(String remoteUserName, final Supplier<T> action) {
    try {
      UserGroupInformation proxyUser = ImpersonationUtil.createProxyUgi(remoteUserName);
      return proxyUser.doAs((PrivilegedExceptionAction<T>) action::get);
    } catch (Exception e) {
      throw new DrillRuntimeException(e);
    }
  }

  /**
   * Only thick-client is currently supported due to a shaded Avatica conflict created by the thin client.
   */
  private void loadDriverClass() throws SQLException {
    try {
      Class.forName(PhoenixStoragePluginConfig.FAT_DRIVER_CLASS);
    } catch (ClassNotFoundException e) {
      throw new SQLException("Cause by : " + e.getMessage());
    }
  }

  /**
   * Override these parameters at any time using the storage configuration.
   *
   * @return the final connection properties
   */
  private Properties useConfProperties() {
    Properties props = new Properties();
    if (getConnectionProperties() != null) {
      props.putAll(getConnectionProperties());
    }
    props.putIfAbsent("phoenix.trace.frequency", "never");
    props.putIfAbsent("phoenix.query.timeoutMs", 30000);
    props.putIfAbsent("phoenix.query.keepAliveMs", 120000);
    props.putIfAbsent("phoenix.schema.isNamespaceMappingEnabled", "true");
    return props;
  }
}
