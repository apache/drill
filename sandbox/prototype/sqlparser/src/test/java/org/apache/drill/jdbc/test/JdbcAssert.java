/*******************************************************************************
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
 ******************************************************************************/
package org.apache.drill.jdbc.test;

import com.google.common.base.Function;
import junit.framework.Assert;
import org.apache.drill.common.util.Hook;

import java.sql.*;
import java.util.Properties;

/**
 * Fluent interface for writing JDBC and query-planning tests.
 */
public class JdbcAssert {
  public static One withModel(String model, String schema) {
    final Properties info = new Properties();
    info.setProperty("schema", schema);
    info.setProperty("model", "inline:" + model);
    return new One(info);
  }

  static String toString(ResultSet resultSet) throws SQLException {
    StringBuilder buf = new StringBuilder();
    while (resultSet.next()) {
      int n = resultSet.getMetaData().getColumnCount();
      String sep = "";
      for (int i = 1; i <= n; i++) {
        buf.append(sep)
            .append(resultSet.getMetaData().getColumnLabel(i))
            .append("=")
            .append(resultSet.getObject(i));
        sep = "; ";
      }
      buf.append("\n");
    }
    return buf.toString();
  }

  public static class One {
    private final Properties info;
    private final ConnectionFactory connectionFactory;

    public One(Properties info) {
      this.info = info;
      this.connectionFactory = new ConnectionFactory() {
        public Connection createConnection() throws Exception {
          Class.forName("org.apache.drill.jdbc.Driver");
          return DriverManager.getConnection("jdbc:drill:", One.this.info);
        }
      };
    }

    public Two sql(String sql) {
      return new Two(connectionFactory, sql);
    }

    public <T> T withConnection(Function<Connection, T> function)
        throws Exception {
      Connection connection = null;
      try {
        connection = connectionFactory.createConnection();
        return function.apply(connection);
      } finally {
        if (connection != null) {
          connection.close();
        }
      }
    }
  }

  public static class Two {
    private final ConnectionFactory connectionFactory;
    private final String sql;

    Two(ConnectionFactory connectionFactory, String sql) {
      this.connectionFactory = connectionFactory;
      this.sql = sql;
    }

    public Two returns(String expected) throws Exception {
      Connection connection = null;
      Statement statement = null;
      try {
        connection = connectionFactory.createConnection();
        statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sql);
        Assert.assertEquals(expected, JdbcAssert.toString(resultSet));
        resultSet.close();
        return this;
      } finally {
        if (statement != null) {
          statement.close();
        }
        if (connection != null) {
          connection.close();
        }
      }
    }

    public void plainContains(String expected) {
      final String[] plan0 = {null};
      Connection connection = null;
      Statement statement = null;
      Hook.Closeable x =
          Hook.LOGICAL_PLAN.add(
              new Function<String, Void>() {
                public Void apply(String o) {
                  plan0[0] = o;
                  return null;
                }
              });
      try {
        connection = connectionFactory.createConnection();
        statement = connection.prepareStatement(sql);
        statement.close();
        final String plan = plan0[0];
        Assert.assertTrue(plan, plan.contains(expected));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  private static interface ConnectionFactory {
    Connection createConnection() throws Exception;
  }
}

// End JdbcAssert.java
