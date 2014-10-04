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
package org.apache.drill.jdbc.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import net.hydromatic.linq4j.Ord;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.logical.LogicalPlan;
import org.apache.drill.common.logical.data.LogicalOperator;
import org.apache.drill.common.util.Hook;
import org.junit.Assert;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSet.Builder;
import com.google.common.collect.Iterables;

/**
 * Fluent interface for writing JDBC and query-planning tests.
 */
public class JdbcAssert {
  public static ModelAndSchema withModel(String model, String schema) {
    final Properties info = new Properties();
    info.setProperty("schema", schema);
    info.setProperty("model", "inline:" + model);
    return new ModelAndSchema(info);
  }

  public static ModelAndSchema withFull(String schema) {
    final Properties info = new Properties();
    info.setProperty("schema", schema);
    return new ModelAndSchema(info);
  }

  public static ModelAndSchema withNoDefaultSchema() {
    return new ModelAndSchema();
  }

  static String toString(ResultSet resultSet, int expectedRecordCount) throws SQLException {
    StringBuilder buf = new StringBuilder();
    int total = 0, n;
    while (resultSet.next()) {
      n = resultSet.getMetaData().getColumnCount();
      total++;
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

  static String toString(ResultSet resultSet) throws SQLException {
    StringBuilder buf = new StringBuilder();
    final List<Ord<String>> columns = columnLabels(resultSet);
    while (resultSet.next()) {
      for (Ord<String> column : columns) {
        buf.append(column.i == 1 ? "" : "; ").append(column.e).append("=").append(resultSet.getObject(column.i));
      }
      buf.append("\n");
    }
    return buf.toString();
  }

  static Set<String> toStringSet(ResultSet resultSet) throws SQLException {
    Builder<String> builder = ImmutableSet.builder();
    final List<Ord<String>> columns = columnLabels(resultSet);
    while (resultSet.next()) {
      StringBuilder buf = new StringBuilder();
      for (Ord<String> column : columns) {
        buf.append(column.i == 1 ? "" : "; ").append(column.e).append("=").append(resultSet.getObject(column.i));
      }
      builder.add(buf.toString());
      buf.setLength(0);
    }
    return builder.build();
  }

  static List<String> toStrings(ResultSet resultSet) throws SQLException {
    final List<String> list = new ArrayList<>();
    StringBuilder buf = new StringBuilder();
    final List<Ord<String>> columns = columnLabels(resultSet);
    while (resultSet.next()) {
      buf.setLength(0);
      for (Ord<String> column : columns) {
        buf.append(column.i == 1 ? "" : "; ").append(column.e).append("=").append(resultSet.getObject(column.i));
      }
      list.add(buf.toString());
    }
    return list;
  }

  private static List<Ord<String>> columnLabels(ResultSet resultSet) throws SQLException {
    int n = resultSet.getMetaData().getColumnCount();
    List<Ord<String>> columns = new ArrayList<>();
    for (int i = 1; i <= n; i++) {
      columns.add(Ord.of(i, resultSet.getMetaData().getColumnLabel(i)));
    }
    return columns;
  }

  public static class ModelAndSchema {
    private final Properties info;
    private final ConnectionFactory connectionFactory;

    public ModelAndSchema() {
      this(null);
    }

    public ModelAndSchema(Properties info) {
      this.info = info;
      this.connectionFactory = new ConnectionFactory() {
        public Connection createConnection() throws Exception {
          Class.forName("org.apache.drill.jdbc.Driver");
          return DriverManager.getConnection("jdbc:drill:zk=local", ModelAndSchema.this.info);
        }
      };
    }

    public TestDataConnection sql(String sql) {
      return new TestDataConnection(connectionFactory, sql);
    }

    public <T> T withConnection(Function<Connection, T> function) throws Exception {
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

  public static class TestDataConnection {
    private final ConnectionFactory connectionFactory;
    private final String sql;

    TestDataConnection(ConnectionFactory connectionFactory, String sql) {
      this.connectionFactory = connectionFactory;
      this.sql = sql;
    }

    /**
     * Checks that the current SQL statement returns the expected result.
     */
    public TestDataConnection returns(String expected) throws Exception {
      Connection connection = null;
      Statement statement = null;
      try {
        connection = connectionFactory.createConnection();
        statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sql);
        expected = expected.trim();
        String result = JdbcAssert.toString(resultSet).trim();
        resultSet.close();

        if (!expected.equals(result)) {
          Assert.fail(String.format("Generated string:\n%s\ndoes not match:\n%s", result, expected));
        }
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

    public TestDataConnection returnsSet(Set<String> expected) throws Exception {
      Connection connection = null;
      Statement statement = null;
      try {
        connection = connectionFactory.createConnection();
        statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sql);
        Set<String> result = JdbcAssert.toStringSet(resultSet);
        resultSet.close();

        if (!expected.equals(result)) {
          Assert.fail(String.format("Generated set:\n%s\ndoes not match:\n%s", result, expected));
        }
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


    /**
     * Checks that the current SQL statement returns the expected result lines. Lines are compared unordered; the test
     * succeeds if the query returns these lines in any order.
     */
    public TestDataConnection returnsUnordered(String... expecteds) throws Exception {
      Connection connection = null;
      Statement statement = null;
      try {
        connection = connectionFactory.createConnection();
        statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sql);
        Assert.assertEquals(unsortedList(Arrays.asList(expecteds)), unsortedList(JdbcAssert.toStrings(resultSet)));
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

    public TestDataConnection displayResults(int recordCount) throws Exception {
      // record count check is done in toString method

      Connection connection = null;
      Statement statement = null;
      try {
        connection = connectionFactory.createConnection();
        statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sql);
        System.out.println(JdbcAssert.toString(resultSet, recordCount));
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

    private SortedSet<String> unsortedList(List<String> strings) {
      final SortedSet<String> set = new TreeSet<>();
      for (String string : strings) {
        set.add(string + "\n");
      }
      return set;
    }

    public LogicalPlan logicalPlan() {
      final String[] plan0 = {null};
      Connection connection = null;
      Statement statement = null;
      final Hook.Closeable x = Hook.LOGICAL_PLAN.add(new Function<String, Void>() {
        public Void apply(String o) {
          plan0[0] = o;
          return null;
        }
      });
      try {
        connection = connectionFactory.createConnection();
        statement = connection.prepareStatement(sql);
        statement.close();
        final String plan = plan0[0].trim();
        return LogicalPlan.parse(DrillConfig.create(), plan);
      } catch (Exception e) {
        throw new RuntimeException(e);
      } finally {
        if (statement != null) {
          try {
            statement.close();
          } catch (SQLException e) {
            // ignore
          }
        }
        if (connection != null) {
          try {
            connection.close();
          } catch (SQLException e) {
            // ignore
          }
        }
        x.close();
      }
    }

    public <T extends LogicalOperator> T planContains(final Class<T> operatorClazz) {
      return (T) Iterables.find(logicalPlan().getSortedOperators(), new Predicate<LogicalOperator>() {
        @Override
        public boolean apply(LogicalOperator input) {
          return input.getClass().equals(operatorClazz);
        }
      });
    }
  }

  private static interface ConnectionFactory {
    Connection createConnection() throws Exception;
  }
}

// End JdbcAssert.java
