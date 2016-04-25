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
package org.apache.drill;

import static java.lang.String.format;
import static org.apache.drill.TestBuilder.listOf;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.drill.exec.store.dfs.WorkspaceSchemaFactory;
import org.junit.Test;

public class TestSelectWithOption extends BaseTestQuery {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(WorkspaceSchemaFactory.class);

  private File genCSVFile(String name, String... rows) throws IOException {
    File file = new File(format("target/%s_%s.csv", this.getClass().getName(), name));
    try (FileWriter fw = new FileWriter(file)) {
      for (int i = 0; i < rows.length; i++) {
        fw.append(rows[i] + "\n");
      }
    }
    return file;
  }

  private String genCSVTable(String name, String... rows) throws IOException {
    File f = genCSVFile(name, rows);
    return format("dfs.`${WORKING_PATH}/%s`", f.getPath());
  }

  private void testWithResult(String query, Object... expectedResult) throws Exception {
    TestBuilder builder = testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("columns");
    for (Object o : expectedResult) {
      builder = builder.baselineValues(o);
    }
    builder.build().run();
  }

  @Test
  public void testTextFieldDelimiter() throws Exception {
    String tableName = genCSVTable("testTextFieldDelimiter",
        "\"b\"|\"0\"",
        "\"b\"|\"1\"",
        "\"b\"|\"2\"");

    String queryTemplate =
        "select columns from table(%s (type => 'TeXT', fieldDelimiter => '%s'))";
    testWithResult(format(queryTemplate, tableName, ","),
        listOf("b\"|\"0"),
        listOf("b\"|\"1"),
        listOf("b\"|\"2")
      );
    testWithResult(format(queryTemplate, tableName, "|"),
        listOf("b", "0"),
        listOf("b", "1"),
        listOf("b", "2")
      );
  }

  @Test
  public void testTabFieldDelimiter() throws Exception {
    String tableName = genCSVTable("testTabFieldDelimiter",
        "1\ta",
        "2\tb");
    String fieldDelimiter = new String(new char[]{92, 116}); // represents \t
    testWithResult(format("select columns from table(%s(type=>'TeXT', fieldDelimiter => '%s'))", tableName, fieldDelimiter),
        listOf("1", "a"),
        listOf("2", "b"));
  }

  @Test
  public void testSingleTextLineDelimiter() throws Exception {
    String tableName = genCSVTable("testSingleTextLineDelimiter",
        "a|b|c");

    testWithResult(format("select columns from table(%s(type => 'TeXT', lineDelimiter => '|'))", tableName),
        listOf("a"),
        listOf("b"),
        listOf("c"));
  }

  @Test
  // '\n' is treated as standard delimiter
  // if user has indicated custom line delimiter but input file contains '\n', split will occur on both
  public void testCustomTextLineDelimiterAndNewLine() throws Exception {
    String tableName = genCSVTable("testTextLineDelimiter",
        "b|1",
        "b|2");

    testWithResult(format("select columns from table(%s(type => 'TeXT', lineDelimiter => '|'))", tableName),
        listOf("b"),
        listOf("1"),
        listOf("b"),
        listOf("2"));
  }

  @Test
  public void testTextLineDelimiterWithCarriageReturn() throws Exception {
    String tableName = genCSVTable("testTextLineDelimiterWithCarriageReturn",
        "1, a\r",
        "2, b\r");
    String lineDelimiter = new String(new char[]{92, 114, 92, 110}); // represents \r\n
    testWithResult(format("select columns from table(%s(type=>'TeXT', lineDelimiter => '%s'))", tableName, lineDelimiter),
        listOf("1, a"),
        listOf("2, b"));
  }

  @Test
  public void testMultiByteLineDelimiter() throws Exception {
    String tableName = genCSVTable("testMultiByteLineDelimiter",
        "1abc2abc3abc");
    testWithResult(format("select columns from table(%s(type=>'TeXT', lineDelimiter => 'abc'))", tableName),
        listOf("1"),
        listOf("2"),
        listOf("3"));
  }

  @Test
  public void testDataWithPartOfMultiByteLineDelimiter() throws Exception {
    String tableName = genCSVTable("testDataWithPartOfMultiByteLineDelimiter",
        "ab1abc2abc3abc");
    testWithResult(format("select columns from table(%s(type=>'TeXT', lineDelimiter => 'abc'))", tableName),
        listOf("ab1"),
        listOf("2"),
        listOf("3"));
  }

  @Test
  public void testTextQuote() throws Exception {
    String tableName = genCSVTable("testTextQuote",
        "\"b\"|\"0\"",
        "\"b\"|\"1\"",
        "\"b\"|\"2\"");

    testWithResult(format("select columns from table(%s(type => 'TeXT', fieldDelimiter => '|', quote => '@'))", tableName),
        listOf("\"b\"", "\"0\""),
        listOf("\"b\"", "\"1\""),
        listOf("\"b\"", "\"2\"")
        );

    String quoteTableName = genCSVTable("testTextQuote2",
        "@b@|@0@",
        "@b$@c@|@1@");
    // It seems that a parameter can not be called "escape"
    testWithResult(format("select columns from table(%s(`escape` => '$', type => 'TeXT', fieldDelimiter => '|', quote => '@'))", quoteTableName),
        listOf("b", "0"),
        listOf("b$@c", "1") // shouldn't $ be removed here?
        );
  }

  @Test
  public void testTextComment() throws Exception {
      String commentTableName = genCSVTable("testTextComment",
          "b|0",
          "@ this is a comment",
          "b|1");
      testWithResult(format("select columns from table(%s(type => 'TeXT', fieldDelimiter => '|', comment => '@'))", commentTableName),
          listOf("b", "0"),
          listOf("b", "1")
          );
  }

  @Test
  public void testTextHeader() throws Exception {
    String headerTableName = genCSVTable("testTextHeader",
        "b|a",
        "b|0",
        "b|1");
    testWithResult(format("select columns from table(%s(type => 'TeXT', fieldDelimiter => '|', skipFirstLine => true))", headerTableName),
        listOf("b", "0"),
        listOf("b", "1")
        );

    testBuilder()
        .sqlQuery(format("select a, b from table(%s(type => 'TeXT', fieldDelimiter => '|', extractHeader => true))", headerTableName))
        .ordered()
        .baselineColumns("b", "a")
        .baselineValues("b", "0")
        .baselineValues("b", "1")
        .build().run();
  }

  @Test
  public void testVariationsCSV() throws Exception {
    String csvTableName = genCSVTable("testVariationsCSV",
        "a,b",
        "c|d");
    // Using the defaults in TextFormatConfig (the field delimiter is neither "," not "|")
    String[] csvQueries = {
//        format("select columns from %s ('TeXT')", csvTableName),
//        format("select columns from %s('TeXT')", csvTableName),
        format("select columns from table(%s ('TeXT'))", csvTableName),
        format("select columns from table(%s (type => 'TeXT'))", csvTableName),
//        format("select columns from %s (type => 'TeXT')", csvTableName)
    };
    for (String csvQuery : csvQueries) {
      testWithResult(csvQuery,
          listOf("a,b"),
          listOf("c|d"));
    }
    // the drill config file binds .csv to "," delimited
    testWithResult(format("select columns from %s", csvTableName),
          listOf("a", "b"),
          listOf("c|d"));
    // setting the delimiter
    testWithResult(format("select columns from table(%s (type => 'TeXT', fieldDelimiter => ','))", csvTableName),
        listOf("a", "b"),
        listOf("c|d"));
    testWithResult(format("select columns from table(%s (type => 'TeXT', fieldDelimiter => '|'))", csvTableName),
        listOf("a,b"),
        listOf("c", "d"));
  }

  @Test
  public void testVariationsJSON() throws Exception {
    String jsonTableName = genCSVTable("testVariationsJSON",
        "{\"columns\": [\"f\",\"g\"]}");
    // the extension is actually csv
    testWithResult(format("select columns from %s", jsonTableName),
        listOf("{\"columns\": [\"f\"", "g\"]}\n")
        );
    String[] jsonQueries = {
        format("select columns from table(%s ('JSON'))", jsonTableName),
        format("select columns from table(%s(type => 'JSON'))", jsonTableName),
//        format("select columns from %s ('JSON')", jsonTableName),
//        format("select columns from %s (type => 'JSON')", jsonTableName),
//        format("select columns from %s(type => 'JSON')", jsonTableName),
        // we can use named format plugin configurations too!
        format("select columns from table(%s(type => 'Named', name => 'json'))", jsonTableName),
    };
    for (String jsonQuery : jsonQueries) {
      testWithResult(jsonQuery, listOf("f","g"));
    }
  }

  @Test
  public void testUse() throws Exception {
    File f = genCSVFile("testUse",
        "{\"columns\": [\"f\",\"g\"]}");
    String jsonTableName = format("`${WORKING_PATH}/%s`", f.getPath());
    // the extension is actually csv
    test("use dfs");
    try {
      String[] jsonQueries = {
          format("select columns from table(%s ('JSON'))", jsonTableName),
          format("select columns from table(%s(type => 'JSON'))", jsonTableName),
      };
      for (String jsonQuery : jsonQueries) {
        testWithResult(jsonQuery, listOf("f","g"));
      }

      testWithResult(format("select length(columns[0]) as columns from table(%s ('JSON'))", jsonTableName), 1L);
    } finally {
      test("use sys");
    }
  }
}
