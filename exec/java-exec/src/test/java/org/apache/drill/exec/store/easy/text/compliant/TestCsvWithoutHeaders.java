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
package org.apache.drill.exec.store.easy.text.compliant;

import java.io.IOException;

import org.apache.drill.categories.RowSetTests;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.test.rowSet.RowSet;
import org.apache.drill.test.rowSet.RowSetBuilder;
import org.apache.drill.test.rowSet.RowSetUtilities;

import static org.apache.drill.test.rowSet.RowSetUtilities.strArray;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

// CSV reader now hosted on the row set framework
@Category(RowSetTests.class)
public class TestCsvWithoutHeaders extends BaseCsvTest {

  private static final String TEST_FILE_NAME = "simple.csv";

  private static String sampleData[] = {
      "10,foo,bar",
      "20,fred,wilma"
  };

  private static String raggedRows[] = {
      "10,dino",
      "20,foo,bar",
      "30"
  };

  @BeforeClass
  public static void setup() throws Exception {
    BaseCsvTest.setup(false,  false);

    buildFile(TEST_FILE_NAME, sampleData);
  }

  @Test
  public void testWildcard() throws IOException {
    try {
      enableV3(false);
      doTestWildcard();
      enableV3(true);
      doTestWildcard();
    } finally {
      resetV3();
    }
  }

  /**
   * Verify that the wildcard expands to the `columns` array
   */

  private void doTestWildcard() throws IOException {
    String sql = "SELECT * FROM `dfs.data`.`%s`";
    RowSet actual = client.queryBuilder().sql(sql, TEST_FILE_NAME).rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
        .addArray("columns", MinorType.VARCHAR)
        .buildSchema();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .addSingleCol(strArray("10", "foo", "bar"))
        .addSingleCol(strArray("20", "fred", "wilma"))
        .build();
    RowSetUtilities.verify(expected, actual);
  }

  @Test
  public void testColumns() throws IOException {
    try {
      enableV3(false);
      doTestColumns();
      enableV3(true);
      doTestColumns();
    } finally {
      resetV3();
    }
  }

  private void doTestColumns() throws IOException {
    String sql = "SELECT columns FROM `dfs.data`.`%s`";
    RowSet actual = client.queryBuilder().sql(sql, TEST_FILE_NAME).rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
        .addArray("columns", MinorType.VARCHAR)
        .buildSchema();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .addSingleCol(strArray("10", "foo", "bar"))
        .addSingleCol(strArray("20", "fred", "wilma"))
        .build();
    RowSetUtilities.verify(expected, actual);
  }

  @Test
  public void doTestWildcardAndMetadataV2() throws IOException {
    try {
      enableV3(false);
      String sql = "SELECT *, filename FROM `dfs.data`.`%s`";
      RowSet actual = client.queryBuilder().sql(sql, TEST_FILE_NAME).rowSet();

      TupleMetadata expectedSchema = new SchemaBuilder()
          .addArray("columns", MinorType.VARCHAR)
          .addNullable("filename", MinorType.VARCHAR)
          .buildSchema();

      RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
          .addRow(strArray("10", "foo", "bar"), TEST_FILE_NAME)
          .addRow(strArray("20", "fred", "wilma"), TEST_FILE_NAME)
          .build();
      RowSetUtilities.verify(expected, actual);
    } finally {
      resetV3();
    }
  }

  @Test
  public void doTestWildcardAndMetadataV3() throws IOException {
    try {
      enableV3(true);
      String sql = "SELECT *, filename FROM `dfs.data`.`%s`";
      RowSet actual = client.queryBuilder().sql(sql, TEST_FILE_NAME).rowSet();

      TupleMetadata expectedSchema = new SchemaBuilder()
          .addArray("columns", MinorType.VARCHAR)
          .add("filename", MinorType.VARCHAR)
          .buildSchema();

      RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
          .addRow(strArray("10", "foo", "bar"), TEST_FILE_NAME)
          .addRow(strArray("20", "fred", "wilma"), TEST_FILE_NAME)
          .build();
      RowSetUtilities.verify(expected, actual);
    } finally {
      resetV3();
    }
  }

  @Test
  public void testColumnsAndMetadataV2() throws IOException {
    try {
      enableV3(false);
      String sql = "SELECT columns, filename FROM `dfs.data`.`%s`";
      RowSet actual = client.queryBuilder().sql(sql, TEST_FILE_NAME).rowSet();

      TupleMetadata expectedSchema = new SchemaBuilder()
          .addArray("columns", MinorType.VARCHAR)
          .addNullable("filename", MinorType.VARCHAR)
          .buildSchema();

      RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
          .addRow(strArray("10", "foo", "bar"), TEST_FILE_NAME)
          .addRow(strArray("20", "fred", "wilma"), TEST_FILE_NAME)
          .build();
      RowSetUtilities.verify(expected, actual);
    } finally {
      resetV3();
    }
  }

  @Test
  public void testColumnsAndMetadataV3() throws IOException {
    try {
      enableV3(true);
      String sql = "SELECT columns, filename FROM `dfs.data`.`%s`";
      RowSet actual = client.queryBuilder().sql(sql, TEST_FILE_NAME).rowSet();

      TupleMetadata expectedSchema = new SchemaBuilder()
          .addArray("columns", MinorType.VARCHAR)
          .add("filename", MinorType.VARCHAR)
          .buildSchema();

      RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
          .addRow(strArray("10", "foo", "bar"), TEST_FILE_NAME)
          .addRow(strArray("20", "fred", "wilma"), TEST_FILE_NAME)
          .build();
      RowSetUtilities.verify(expected, actual);
    } finally {
      resetV3();
    }
  }

  @Test
  public void testSpecificColumns() throws IOException {
    try {
      enableV3(false);
      doTestSpecificColumns();
      enableV3(true);
      doTestSpecificColumns();
    } finally {
      resetV3();
    }
  }

  private void doTestSpecificColumns() throws IOException {
    String sql = "SELECT columns[0], columns[2] FROM `dfs.data`.`%s`";
    RowSet actual = client.queryBuilder().sql(sql, TEST_FILE_NAME).rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
        .addNullable("EXPR$0", MinorType.VARCHAR)
        .addNullable("EXPR$1", MinorType.VARCHAR)
        .buildSchema();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .addRow("10", "bar")
        .addRow("20", "wilma")
        .build();
    RowSetUtilities.verify(expected, actual);
  }

  @Test
  public void testRaggedRows() throws IOException {
    try {
      enableV3(false);
      doTestRaggedRows();
      enableV3(true);
      doTestRaggedRows();
    } finally {
      resetV3();
    }
  }

  private void doTestRaggedRows() throws IOException {
    String fileName = "ragged.csv";
    buildFile(fileName, raggedRows);
    String sql = "SELECT columns FROM `dfs.data`.`%s`";
    RowSet actual = client.queryBuilder().sql(sql, fileName).rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
        .addArray("columns", MinorType.VARCHAR)
        .buildSchema();
    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .addSingleCol(strArray("10", "dino"))
        .addSingleCol(strArray("20", "foo", "bar"))
        .addSingleCol(strArray("30"))
        .build();
    RowSetUtilities.verify(expected, actual);
  }
}
