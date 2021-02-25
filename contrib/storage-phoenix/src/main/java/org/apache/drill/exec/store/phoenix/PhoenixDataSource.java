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
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;

import javax.sql.DataSource;

import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;

/**
 * Phoenix’s Connection objects are different from most other JDBC Connections
 * due to the underlying HBase connection. The Phoenix Connection object
 * is designed to be a thin object that is inexpensive to create.
 *
 * If Phoenix Connections are reused, it is possible that the underlying HBase connection
 * is not always left in a healthy state by the previous user. It is better to
 * create new Phoenix Connections to ensure that you avoid any potential issues.
 */
public class PhoenixDataSource implements DataSource {

  private static final String DEFAULT_URL_HEADER = "jdbc:phoenix:thin:url=http://";
  private static final String DEFAULT_SERIALIZATION = "serialization=PROTOBUF";

  private String url;
  private Map<String, Object> connectionProperties;
  private boolean isFatClient; // Is a fat client

  public PhoenixDataSource(String url) {
    Preconditions.checkNotNull(url);
    this.url = url;
  }

  public PhoenixDataSource(String host, int port) {
    Preconditions.checkNotNull(host);
    Preconditions.checkArgument(port > 0, "Please set the correct port.");
    this.url = new StringBuilder()
        .append(DEFAULT_URL_HEADER)
        .append(host)
        .append(":")
        .append(port)
        .append(";")
        .append(DEFAULT_SERIALIZATION)
        .toString();
  }

  public PhoenixDataSource(String url, Map<String, Object> connectionProperties) {
    this(url);
    Preconditions.checkNotNull(connectionProperties);
    connectionProperties.forEach((k, v)
        -> Preconditions.checkArgument(v != null, String.format("does not accept null values : %s", k)));
    this.connectionProperties = connectionProperties;
  }

  public PhoenixDataSource(String host, int port, Map<String, Object> connectionProperties) {
    this(host, port);
    Preconditions.checkNotNull(connectionProperties);
    connectionProperties.forEach((k, v)
        -> Preconditions.checkArgument(v != null, String.format("does not accept null values : %s", k)));
    this.connectionProperties = connectionProperties;
  }

  public Map<String, Object> getConnectionProperties() {
    return connectionProperties;
  }

  public void setConnectionProperties(Map<String, Object> connectionProperties) {
    this.connectionProperties = connectionProperties;
  }

  @Override
  public PrintWriter getLogWriter() throws SQLException {
    throw new UnsupportedOperationException("getLogWriter");
  }

  @Override
  public void setLogWriter(PrintWriter out) throws SQLException {
    throw new UnsupportedOperationException("setLogWriter");
  }

  @Override
  public void setLoginTimeout(int seconds) throws SQLException {
    throw new UnsupportedOperationException("setLoginTimeout");
  }

  @Override
  public int getLoginTimeout() throws SQLException {
    return 0;
  }

  @Override
  public Logger getParentLogger() throws SQLFeatureNotSupportedException {
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
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return iface.isInstance(this);
  }

  @Override
  public Connection getConnection() throws SQLException {
    useDriverClass();
    Connection conn = DriverManager.getConnection(url, useConfProperties());
    return conn;
  }

  @Override
  public Connection getConnection(String username, String password) throws SQLException {
    useDriverClass();
    Connection conn = DriverManager.getConnection(url, username, password);
    return conn;
  }

  /**
   * The thin-client is lightweight and better compatibility.
   * Only thin-client is currently supported.
   *
   * @throws SQLException
   */
  private void useDriverClass() throws SQLException {
    try {
      if (!isFatClient) {
        Class.forName(PhoenixStoragePluginConfig.THIN_DRIVER_CLASS);
      } else {
        Class.forName(PhoenixStoragePluginConfig.FAT_DRIVER_CLASS);
      }
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
    props.put("phoenix.trace.frequency", "never");
    props.put("phoenix.query.timeoutMs", 30000);
    props.put("phoenix.query.keepAliveMs", 120000);
    if (getConnectionProperties() != null) {
      props.putAll(getConnectionProperties());
    }
    return props;
  }
}
