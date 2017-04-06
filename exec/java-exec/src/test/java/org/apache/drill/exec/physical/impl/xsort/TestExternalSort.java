/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.physical.impl.xsort;

import org.apache.drill.BaseTestQuery;
import org.apache.drill.TestBuilder;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.test.SecondaryTest;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;

@Category(SecondaryTest.class)
public class TestExternalSort extends BaseTestQuery {

  @Test
  public void testNumericTypesManaged() throws Exception {
    testNumericTypes( false );
  }

  @Test
  public void testNumericTypesLegacy() throws Exception {
    testNumericTypes( true );
  }

  private void testNumericTypes(boolean testLegacy) throws Exception {
    final int record_count = 10000;
    String dfs_temp = getDfsTestTmpSchemaLocation();
    System.out.println(dfs_temp);
    File table_dir = new File(dfs_temp, "numericTypes");
    table_dir.mkdir();
    try(BufferedOutputStream os = new BufferedOutputStream(new FileOutputStream(new File(table_dir, "a.json")))) {
      String format = "{ a : %d }%n";
      for (int i = 0; i <= record_count; i += 2) {
        os.write(String.format(format, i).getBytes());
      }
    }
    try(BufferedOutputStream os = new BufferedOutputStream(new FileOutputStream(new File(table_dir, "b.json")))) {
      String format = "{ a : %.2f }%n";
      for (int i = 1; i <= record_count; i+=2) {
        os.write(String.format(format, (float) i).getBytes());
      }
    }
    String query = "select * from dfs_test.tmp.numericTypes order by a desc";
    TestBuilder builder = testBuilder()
            .sqlQuery(query)
            .optionSettingQueriesForTestQuery(getOptions(testLegacy))
            .ordered()
            .baselineColumns("a");
    for (int i = record_count; i >= 0;) {
      builder.baselineValues((long) i--);
      if (i >= 0) {
        builder.baselineValues((double) i--);
      }
    }
    builder.go();
  }

  private String getOptions(boolean testLegacy) {
    String options = "alter session set `exec.enable_union_type` = true";
    options += ";alter session set `" + ExecConstants.EXTERNAL_SORT_DISABLE_MANAGED_OPTION.getOptionName() + "` = " +
        Boolean.toString(testLegacy);
    return options;
  }

  @Test
  @Ignore("Schema changes are disabled in external sort")
  public void testNumericAndStringTypesManaged() throws Exception {
    testNumericAndStringTypes(false);
  }

  @Test
  @Ignore("Schema changes are disabled in external sort")
  public void testNumericAndStringTypesLegacy() throws Exception {
    testNumericAndStringTypes(true);
  }

  private void testNumericAndStringTypes(boolean testLegacy) throws Exception {
    final int record_count = 10000;
    String dfs_temp = getDfsTestTmpSchemaLocation();
    System.out.println(dfs_temp);
    File table_dir = new File(dfs_temp, "numericAndStringTypes");
    table_dir.mkdir();
    try (BufferedOutputStream os = new BufferedOutputStream(new FileOutputStream(new File(table_dir, "a.json")))) {
      String format = "{ a : %d }%n";
      for (int i = 0; i <= record_count; i += 2) {
        os.write(String.format(format, i).getBytes());
      }
    }
    try (BufferedOutputStream os = new BufferedOutputStream(new FileOutputStream(new File(table_dir, "b.json")))) {
      String format = "{ a : \"%05d\" }%n";
      for (int i = 1; i <= record_count; i+=2) {
        os.write(String.format(format, i).getBytes());
      }
    }
    String query = "select * from dfs_test.tmp.numericAndStringTypes order by a desc";
    TestBuilder builder = testBuilder()
            .sqlQuery(query)
            .ordered()
            .optionSettingQueriesForTestQuery(getOptions(testLegacy))
            .baselineColumns("a");
    // Strings come first because order by is desc
    for (int i = record_count; i >= 0;) {
      i--;
      if (i >= 0) {
        builder.baselineValues(String.format("%05d", i--));
      }
    }
    for (int i = record_count; i >= 0;) {
      builder.baselineValues((long) i--);
      i--;
    }
    builder.go();
  }

  @Test
  public void testNewColumnsManaged() throws Exception {
    testNewColumns(false);
  }


  @Test
  public void testNewColumnsLegacy() throws Exception {
    testNewColumns(true);
  }

  private void testNewColumns(boolean testLegacy) throws Exception {
    final int record_count = 10000;
    String dfs_temp = getDfsTestTmpSchemaLocation();
    System.out.println(dfs_temp);
    File table_dir = new File(dfs_temp, "newColumns");
    table_dir.mkdir();
    try (BufferedOutputStream os = new BufferedOutputStream(new FileOutputStream(new File(table_dir, "a.json")))) {
      String format = "{ a : %d, b : %d }%n";
      for (int i = 0; i <= record_count; i += 2) {
        os.write(String.format(format, i, i).getBytes());
      }
    }
    try (BufferedOutputStream os = new BufferedOutputStream(new FileOutputStream(new File(table_dir, "b.json")))) {
      String format = "{ a : %d, c : %d }%n";
      for (int i = 1; i <= record_count; i+=2) {
        os.write(String.format(format, i, i).getBytes());
      }
    }
    String query = "select a, b, c from dfs_test.tmp.newColumns order by a desc";
//    Test framework currently doesn't handle changing schema (i.e. new columns) on the client side
    TestBuilder builder = testBuilder()
            .sqlQuery(query)
            .ordered()
            .optionSettingQueriesForTestQuery(getOptions(testLegacy))
            .baselineColumns("a", "b", "c");
    for (int i = record_count; i >= 0;) {
      builder.baselineValues((long) i, (long) i--, null);
      if (i >= 0) {
        builder.baselineValues((long) i, null, (long) i--);
      }
    }
    builder.go();
    String newQuery = "select * from dfs_test.tmp.newColumns order by a desc";
    test(newQuery);
  }
}
