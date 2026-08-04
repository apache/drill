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
package org.apache.drill.exec.store.accumulo;

import java.nio.charset.StandardCharsets;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for creating test tables and data in Accumulo.
 */
public class AccumuloTestUtils {
  private static final Logger logger = LoggerFactory.getLogger(AccumuloTestUtils.class);

  public static final String TEST_TABLE_1 = "drill_test_table_1";
  public static final String TEST_TABLE_USERS = "drill_test_users";
  public static final String TEST_TABLE_LARGE = "drill_test_large";
  public static final String TEST_TABLE_SPARSE = "drill_test_sparse";

  /**
   * Creates a simple test table with basic key-value data.
   *
   * <p>Table structure:</p>
   * <ul>
   *   <li>row_key: row_001 to row_010</li>
   *   <li>cf:name - string names</li>
   *   <li>cf:age - integer ages as strings</li>
   *   <li>cf:city - city names</li>
   * </ul>
   */
  public static void createTestTable1(AccumuloClient client) throws Exception {
    String tableName = TEST_TABLE_1;
    createTableIfNotExists(client, tableName);

    try (BatchWriter writer = client.createBatchWriter(tableName, new BatchWriterConfig())) {
      String[][] data = {
          {"row_001", "Alice", "30", "New York"},
          {"row_002", "Bob", "25", "Los Angeles"},
          {"row_003", "Charlie", "35", "Chicago"},
          {"row_004", "Diana", "28", "Houston"},
          {"row_005", "Eve", "32", "Phoenix"},
          {"row_006", "Frank", "45", "Philadelphia"},
          {"row_007", "Grace", "29", "San Antonio"},
          {"row_008", "Henry", "38", "San Diego"},
          {"row_009", "Ivy", "26", "Dallas"},
          {"row_010", "Jack", "41", "San Jose"}
      };

      for (String[] row : data) {
        Mutation m = new Mutation(new Text(row[0]));
        m.put(new Text("cf"), new Text("name"), new Value(row[1].getBytes(StandardCharsets.UTF_8)));
        m.put(new Text("cf"), new Text("age"), new Value(row[2].getBytes(StandardCharsets.UTF_8)));
        m.put(new Text("cf"), new Text("city"), new Value(row[3].getBytes(StandardCharsets.UTF_8)));
        writer.addMutation(m);
      }
    }

    logger.info("Created test table: {} with 10 rows", tableName);
  }

  /**
   * Creates a test table with user data and multiple column families.
   *
   * <p>Table structure:</p>
   * <ul>
   *   <li>row_key: user_001 to user_020</li>
   *   <li>personal:first_name, personal:last_name</li>
   *   <li>contact:email, contact:phone</li>
   *   <li>employment:company, employment:title, employment:salary</li>
   * </ul>
   */
  public static void createTestTableUsers(AccumuloClient client) throws Exception {
    String tableName = TEST_TABLE_USERS;
    createTableIfNotExists(client, tableName);

    try (BatchWriter writer = client.createBatchWriter(tableName, new BatchWriterConfig())) {
      String[][] data = {
          {"user_001", "John", "Doe", "john.doe@email.com", "555-0101", "Acme Corp", "Engineer", "75000"},
          {"user_002", "Jane", "Smith", "jane.smith@email.com", "555-0102", "TechCo", "Manager", "95000"},
          {"user_003", "Bob", "Johnson", "bob.j@email.com", "555-0103", "DataInc", "Analyst", "65000"},
          {"user_004", "Alice", "Williams", "alice.w@email.com", "555-0104", "Acme Corp", "Director", "120000"},
          {"user_005", "Charlie", "Brown", "charlie.b@email.com", "555-0105", "TechCo", "Developer", "80000"},
          {"user_006", "Diana", "Davis", "diana.d@email.com", "555-0106", "DataInc", "Scientist", "90000"},
          {"user_007", "Edward", "Miller", "edward.m@email.com", "555-0107", "Acme Corp", "Engineer", "78000"},
          {"user_008", "Fiona", "Wilson", "fiona.w@email.com", "555-0108", "TechCo", "Designer", "72000"},
          {"user_009", "George", "Moore", "george.m@email.com", "555-0109", "DataInc", "Manager", "98000"},
          {"user_010", "Hannah", "Taylor", "hannah.t@email.com", "555-0110", "Acme Corp", "Analyst", "68000"},
          {"user_011", "Ivan", "Anderson", "ivan.a@email.com", "555-0111", "TechCo", "Developer", "82000"},
          {"user_012", "Julia", "Thomas", "julia.t@email.com", "555-0112", "DataInc", "Engineer", "77000"},
          {"user_013", "Kevin", "Jackson", "kevin.j@email.com", "555-0113", "Acme Corp", "Manager", "105000"},
          {"user_014", "Laura", "White", "laura.w@email.com", "555-0114", "TechCo", "Director", "125000"},
          {"user_015", "Michael", "Harris", "michael.h@email.com", "555-0115", "DataInc", "Analyst", "67000"},
          {"user_016", "Nancy", "Martin", "nancy.m@email.com", "555-0116", "Acme Corp", "Developer", "79000"},
          {"user_017", "Oscar", "Garcia", "oscar.g@email.com", "555-0117", "TechCo", "Scientist", "92000"},
          {"user_018", "Patricia", "Martinez", "patricia.m@email.com", "555-0118", "DataInc", "Designer", "71000"},
          {"user_019", "Quincy", "Robinson", "quincy.r@email.com", "555-0119", "Acme Corp", "Engineer", "76000"},
          {"user_020", "Rachel", "Clark", "rachel.c@email.com", "555-0120", "TechCo", "Manager", "99000"}
      };

      for (String[] row : data) {
        Mutation m = new Mutation(new Text(row[0]));
        // personal column family
        m.put(new Text("personal"), new Text("first_name"), new Value(row[1].getBytes(StandardCharsets.UTF_8)));
        m.put(new Text("personal"), new Text("last_name"), new Value(row[2].getBytes(StandardCharsets.UTF_8)));
        // contact column family
        m.put(new Text("contact"), new Text("email"), new Value(row[3].getBytes(StandardCharsets.UTF_8)));
        m.put(new Text("contact"), new Text("phone"), new Value(row[4].getBytes(StandardCharsets.UTF_8)));
        // employment column family
        m.put(new Text("employment"), new Text("company"), new Value(row[5].getBytes(StandardCharsets.UTF_8)));
        m.put(new Text("employment"), new Text("title"), new Value(row[6].getBytes(StandardCharsets.UTF_8)));
        m.put(new Text("employment"), new Text("salary"), new Value(row[7].getBytes(StandardCharsets.UTF_8)));
        writer.addMutation(m);
      }
    }

    logger.info("Created test table: {} with 20 rows", tableName);
  }

  /**
   * Creates a larger test table for testing limit and pagination.
   *
   * <p>Table structure:</p>
   * <ul>
   *   <li>row_key: row_0001 to row_1000</li>
   *   <li>data:value - sequential integer values</li>
   *   <li>data:description - description string</li>
   * </ul>
   */
  public static void createTestTableLarge(AccumuloClient client) throws Exception {
    String tableName = TEST_TABLE_LARGE;
    createTableIfNotExists(client, tableName);

    try (BatchWriter writer = client.createBatchWriter(tableName, new BatchWriterConfig())) {
      for (int i = 1; i <= 1000; i++) {
        String rowKey = String.format("row_%04d", i);
        Mutation m = new Mutation(new Text(rowKey));
        m.put(new Text("data"), new Text("value"), new Value(String.valueOf(i).getBytes(StandardCharsets.UTF_8)));
        m.put(new Text("data"), new Text("description"), new Value(("Item number " + i).getBytes(StandardCharsets.UTF_8)));
        writer.addMutation(m);
      }
    }

    logger.info("Created test table: {} with 1000 rows", tableName);
  }

  /**
   * Creates a test table where rows have different sets of column qualifiers.
   *
   * <p>This exercises the record reader's handling of columns that are absent from
   * some rows: every missing qualifier must come back as NULL rather than shifting
   * values between rows.</p>
   *
   * <p>Table structure:</p>
   * <ul>
   *   <li>sparse_001: cf:a, cf:b, cf:c</li>
   *   <li>sparse_002: cf:a only</li>
   *   <li>sparse_003: cf:b only</li>
   *   <li>sparse_004: cf:c only</li>
   *   <li>sparse_005: cf:a, cf:c</li>
   * </ul>
   */
  public static void createTestTableSparse(AccumuloClient client) throws Exception {
    String tableName = TEST_TABLE_SPARSE;
    createTableIfNotExists(client, tableName);

    try (BatchWriter writer = client.createBatchWriter(tableName, new BatchWriterConfig())) {
      // null means the qualifier is absent for that row
      String[][] data = {
          {"sparse_001", "a1", "b1", "c1"},
          {"sparse_002", "a2", null, null},
          {"sparse_003", null, "b3", null},
          {"sparse_004", null, null, "c4"},
          {"sparse_005", "a5", null, "c5"}
      };

      String[] qualifiers = {"a", "b", "c"};
      for (String[] row : data) {
        Mutation m = new Mutation(new Text(row[0]));
        for (int i = 0; i < qualifiers.length; i++) {
          String value = row[i + 1];
          if (value != null) {
            m.put(new Text("cf"), new Text(qualifiers[i]),
                new Value(value.getBytes(StandardCharsets.UTF_8)));
          }
        }
        writer.addMutation(m);
      }
    }

    logger.info("Created test table: {} with 5 sparse rows", tableName);
  }

  /**
   * Creates all test tables.
   */
  public static void createAllTestTables(AccumuloClient client) throws Exception {
    createTestTable1(client);
    createTestTableUsers(client);
    createTestTableLarge(client);
    createTestTableSparse(client);
  }

  /**
   * Deletes all test tables.
   */
  public static void deleteAllTestTables(AccumuloClient client) throws Exception {
    deleteTableIfExists(client, TEST_TABLE_1);
    deleteTableIfExists(client, TEST_TABLE_USERS);
    deleteTableIfExists(client, TEST_TABLE_LARGE);
    deleteTableIfExists(client, TEST_TABLE_SPARSE);
  }

  /**
   * Creates a table if it doesn't exist.
   */
  public static void createTableIfNotExists(AccumuloClient client, String tableName)
      throws AccumuloException, AccumuloSecurityException {
    try {
      if (!client.tableOperations().exists(tableName)) {
        client.tableOperations().create(tableName);
        logger.debug("Created table: {}", tableName);
      }
    } catch (TableExistsException e) {
      // Table was created by another thread, ignore
      logger.debug("Table {} already exists", tableName);
    }
  }

  /**
   * Deletes a table if it exists.
   */
  public static void deleteTableIfExists(AccumuloClient client, String tableName)
      throws AccumuloException, AccumuloSecurityException {
    try {
      if (client.tableOperations().exists(tableName)) {
        client.tableOperations().delete(tableName);
        logger.debug("Deleted table: {}", tableName);
      }
    } catch (TableNotFoundException e) {
      // Table was deleted by another thread, ignore
      logger.debug("Table {} not found for deletion", tableName);
    }
  }
}
