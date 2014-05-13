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
package org.apache.drill.exec.physical.impl.writer;

import com.google.common.collect.Lists;
import org.apache.drill.BaseTestQuery;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.memory.TopLevelAllocator;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.rpc.user.QueryResultBatch;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.junit.*;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Ignore
public class TestParquetWriter extends BaseTestQuery {

  static FileSystem fs;

  @BeforeClass
  public static void initFs() throws Exception {
    Configuration conf = new Configuration();
    conf.set("fs.name.default", "local");

    fs = FileSystem.get(conf);
  }

  @Test
  public void testSimple() throws Exception {
    String selection = "*";
    String inputTable = "cp.`employee.json`";
    runTestAndValidate(selection, selection, inputTable);
  }

  @Test
  public void testDecimal() throws Exception {
    String selection = "cast(salary as decimal(8,2)) as decimal8, cast(salary as decimal(15,2)) as decimal15, " +
            "cast(salary as decimal(24,2)) as decimal24, cast(salary as decimal(38,2)) as decimal38";
    String validateSelection = "decimal8, decimal15, decimal24, decimal38";
    String inputTable = "cp.`employee.json`";
    runTestAndValidate(selection, validateSelection, inputTable);
  }

  @Test
  @Ignore //this test currently fails. will file jira
  public void testMulipleRowGroups() throws Exception {
    try {
      test(String.format("ALTER SESSION SET `%s` = %d", ExecConstants.PARQUET_BLOCK_SIZE, 512*1024));
      String selection = "*";
      String inputTable = "cp.`customer.json`";
      runTestAndValidate(selection, selection, inputTable);
    } finally {
      test(String.format("ALTER SESSION SET `%s` = %d", ExecConstants.PARQUET_BLOCK_SIZE, 512*1024*1024));
    }
  }


  @Test
  @Ignore //enable once Date is enabled
  public void testDate() throws Exception {
    String selection = "cast(hire_date as DATE) as hire_date";
    String validateSelection = "hire_date";
    String inputTable = "cp.`employee.json`";
    runTestAndValidate(selection, validateSelection, inputTable);
  }

  public void runTestAndValidate(String selection, String validationSelection, String inputTable) throws Exception {

    Path path = new Path("/tmp/drilltest/employee_parquet");
    if (fs.exists(path)) {
      fs.delete(path, true);
    }

    test("use dfs.tmp");
    String query = String.format("SELECT %s FROM %s", selection, inputTable);
    String create = "CREATE TABLE employee_parquet AS " + query;
    String validateQuery = String.format("SELECT %s FROM employee_parquet", validationSelection);
    test(create);
    List<QueryResultBatch> results = testSqlWithResults(query);
    List<QueryResultBatch> expected = testSqlWithResults(validateQuery);
    compareResults(expected, results);
  }

  public void compareResults(List<QueryResultBatch> expected, List<QueryResultBatch> result) throws Exception {
    Set<Object> expectedObjects = new HashSet();
    Set<Object> actualObjects = new HashSet();

    BatchSchema schema = null;
    RecordBatchLoader loader = new RecordBatchLoader(getAllocator());
    for (QueryResultBatch batch : expected) {
      loader.load(batch.getHeader().getDef(), batch.getData());
      if (schema == null) {
        schema = loader.getSchema();
      }
      for (VectorWrapper w : loader) {
        for (int i = 0; i < loader.getRecordCount(); i++) {
          Object obj = w.getValueVector().getAccessor().getObject(i);
          if (obj != null) {
            if (obj instanceof Text) {
              expectedObjects.add(obj.toString());
              if (obj.toString().equals("")) {
                System.out.println(w.getField());
              }
            } else {
              expectedObjects.add(obj);
            }
          }
        }
      }
      loader.clear();
    }
    for (QueryResultBatch batch : result) {
      loader.load(batch.getHeader().getDef(), batch.getData());
      for (VectorWrapper w : loader) {
        for (int i = 0; i < loader.getRecordCount(); i++) {
          Object obj = w.getValueVector().getAccessor().getObject(i);
          if (obj != null) {
            if (obj instanceof Text) {
              actualObjects.add(obj.toString());
              if (obj.toString().equals(" ")) {
                System.out.println("EMPTY STRING" + w.getField());
              }
            } else {
              actualObjects.add(obj);
            }
          }
        }
      }
      loader.clear();
    }

//    Assert.assertEquals("Different number of objects returned", expectedObjects.size(), actualObjects.size());

    for (Object obj: expectedObjects) {
      Assert.assertTrue(String.format("Expected object %s", obj), actualObjects.contains(obj));
    }
    for (Object obj: actualObjects) {
      Assert.assertTrue(String.format("Unexpected object %s", obj), expectedObjects.contains(obj));
    }
  }
}
