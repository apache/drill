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

import org.junit.Test;

public class TestSelectWithOption extends BaseTestQuery {

//  @Test
//  public void testBar() throws Exception {
//    test("select dfs.`${WORKING_PATH}/some/path`() from cp.`tpch/region.parquet`");
//  }

  @Test
  public void testText() throws Exception {
    File input = new File("target/" + this.getClass().getName() + ".csv");
    String query = "select columns from table(dfs.`${WORKING_PATH}/" + input.getPath() +
        "`(type => 'TeXT', fieldDelimiter => '%s'))";
    String queryComma = format(query, ",");
    String queryPipe = format(query, "|");
    System.out.println(queryComma);
    System.out.println(queryPipe);
    TestBuilder builderComma = testBuilder()
        .sqlQuery(queryComma)
        .ordered()
        .baselineColumns("columns");
    TestBuilder builderPipe = testBuilder()
        .sqlQuery(queryPipe)
        .ordered()
        .baselineColumns("columns");
    try (FileWriter fw = new FileWriter(input)) {
//      fw.append("a|b\n");
      for (int i = 0; i < 3; i++) {
        fw.append("\"b\"|\"" + i + "\"\n");
        builderComma = builderComma.baselineValues(listOf("b\"|\"" + i));
        builderPipe = builderPipe.baselineValues(listOf("b", String.valueOf(i)));
      }
    }

    test("select columns from dfs.`${WORKING_PATH}/" + input.getPath() + "`");

    builderComma.build().run();
    builderPipe.build().run();
  }

  @Test
  public void testParquetFailure() throws Exception {
    File input = new File("target/" + this.getClass().getName() + ".csv");
    try (FileWriter fw = new FileWriter(input)) {
//      fw.append("a|b\n");
      for (int i = 0; i < 3; i++) {
        fw.append("\"b\"|\"" + i + "\"\n");
      }
    }

    String query = "select columns from table(dfs.`${WORKING_PATH}/" + input.getPath() +
        "`(type => 'PARQUET'))";
    System.out.println(query);

    test(query);

  }

}
