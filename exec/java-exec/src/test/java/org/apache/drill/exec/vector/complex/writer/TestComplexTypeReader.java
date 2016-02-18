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

package org.apache.drill.exec.vector.complex.writer;

import org.apache.drill.BaseTestQuery;
import org.apache.drill.TestBuilder;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.apache.drill.TestBuilder.listOf;

public class TestComplexTypeReader extends BaseTestQuery{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestComplexTypeReader.class);

  @BeforeClass
  public static void init() throws Exception {
    testNoResult("alter session set `exec.enable_union_type` = true");
  }

  @Test
  // Repeated map (map) -> json.
  public void testX() throws Exception{
    test("select convert_to(z[0], 'JSON') from cp.`jsoninput/input2.json`;");
  }

  @Test
  //map -> json.
  public void testX2() throws Exception{
    test("select convert_to(x, 'JSON') from cp.`jsoninput/input2.json`;");
  }

  @Test
  //Map (mapfield) -> json.
  public void testX3() throws Exception{
    test("select convert_to(tbl.x.y, 'JSON') from cp.`jsoninput/input2.json` tbl;");
  }

  @Test
  //float value -> json
  public void testX4() throws Exception{
    test("select convert_to(`float`, 'JSON') from cp.`jsoninput/input2.json`;");
  }

  @Test
  //integer value -> json
  public void testX5() throws Exception{
    test("select convert_to(`integer`, 'JSON') from cp.`jsoninput/input2.json`;");
  }

  @Test
  // repeated map -> json.
  public void testX6() throws Exception{
    test("select convert_to(z, 'JSON')  from cp.`jsoninput/input2.json`;");
  }

  @Test
  //repeated list (Repeated BigInt) -> json
  public void testX7() throws Exception{
    test("select convert_to(rl[1], 'JSON') from cp.`jsoninput/input2.json`;");
  }

  @Test
  //repeated list (Repeated BigInt) -> json
  public void testX8() throws Exception{
    test("select convert_to(rl[0][1], 'JSON') from cp.`jsoninput/input2.json`;");
  }

  @Test
  //repeated list -> json
  public void testX9() throws Exception{
    test("select convert_to(rl, 'JSON') from cp.`jsoninput/input2.json`;");
  }

  @Test
  public void testY() throws Exception{
    test("select z[0] from cp.`jsoninput/input2.json`;");
  }

  @Test
  public void testY2() throws Exception{
    test("select x from cp.`jsoninput/input2.json`;");
  }

  @Test
  public void testY3() throws Exception{
    test("select tbl.x.y from cp.`jsoninput/input2.json` tbl;");
  }

  @Test
  public void testY6() throws Exception{
    test("select z  from cp.`jsoninput/input2.json`;");
  }

  @Test
  //repeated list (Repeated BigInt)
  public void testZ() throws Exception{
    test("select rl[1] from cp.`jsoninput/input2.json`;");
  }

  @Test
  //repeated list (Repeated BigInt ( BigInt) ) )
  public void testZ1() throws Exception{
    test("select rl[0][1] from cp.`jsoninput/input2.json`;");
  }

  @Test
  //repeated list (Repeated BigInt ( BigInt) ) ). The first index is out of boundary
  public void testZ2() throws Exception{
    test("select rl[1000][1] from cp.`jsoninput/input2.json`;");
  }

  @Test
  //repeated list (Repeated BigInt ( BigInt) ) ). The second index is out of boundary
  public void testZ3() throws Exception{
    test("select rl[0][1000] from cp.`jsoninput/input2.json`;");
  }

  @Test
  //repeated list. The repeated list is asked for twice, hence requires copying (evaluation in ProjectRecordBatch)
  public void testZ4() throws Exception{
    test("select rl, rl from cp.`jsoninput/input2.json`;");
  }

  @Test
  //repeated map  --> Json.  It will go beyond the buffer of size 256 allocated in setup.
  public void testA0() throws Exception{
    test("  select convert_to(types, 'JSON') from cp.`jsoninput/vvtypes.json`;");
  }

  @Test
  //repeated map (map) --> Json.
  public void testA1() throws Exception{
    test("  select convert_to(types[1], 'JSON') from cp.`jsoninput/vvtypes.json`;");
  }

  @Test
  //repeated map (map (repeated map) ) --> Json.
  public void testA2() throws Exception{
    test("  select convert_to(types[1].minor, 'JSON') from cp.`jsoninput/vvtypes.json`;");
  }

  @Test
  //repeated map (map( repeated map (map (varchar)))) --> Json.
  public void testA3() throws Exception{
    test("  select convert_to(types[1].minor[0].valueHolder, 'JSON') from cp.`jsoninput/vvtypes.json`;");
  }


  @Test
  //Two complex type functions in SELECT clause : repeated map (map) --> Json,
  public void testA4() throws Exception{
    test("  select convert_to(types[1], 'JSON'), convert_to(modes[2], 'JSON') from cp.`jsoninput/vvtypes.json`;");
  }

  @Test
  //repeated map (map) .
  public void testB1() throws Exception{
    test("  select types[1] from cp.`jsoninput/vvtypes.json`;");
  }

  @Test
  //repeated map (map (repeated map) ).
  public void testB2() throws Exception{
    test("  select types[1].minor from cp.`jsoninput/vvtypes.json`;");
  }

  @Test
  //repeated map (map( repeated map (map (varchar)))).
  public void testB3() throws Exception{
    test("  select types[1].minor[0].valueholder from cp.`jsoninput/vvtypes.json`;");
  }

  @Test  // DRILL-1250
  //repeated scalar values evaluation.
  public void test_repeatedList() throws Exception{
    test("select l, l from cp.`jsoninput/input2.json`;");
  }

  @Test
  public void testKeyValueGen() throws Exception {
    test("select kvgen(x) from cp.`jsoninput/input2.json`");
    test("select kvgen(bigintegercol), kvgen(float8col) from cp.`jsoninput/input3.json`");
  }

  @Test
  // Functions tests kvgen functionality where the 'value' part of the map is complex
  public void testKVGenWithComplexValues() throws Exception {
    // test where 'value' is a list of integers
    test("select kvgen(a) from cp.`jsoninput/kvgen_complex_input.json`");

    // test where 'value' is a repeated list of floats
    test("select kvgen(c) from cp.`jsoninput/kvgen_complex_input.json`");

    // test where 'value' is a map
    test("select kvgen(e) from cp.`jsoninput/kvgen_complex_input.json`");

    // test where 'value' is a repeated list of maps
    test("select kvgen(i) from cp.`jsoninput/kvgen_complex_input.json`");

    // test where 'value' is a map that contains a list
    test("select kvgen(m) from cp.`jsoninput/kvgen_complex_input.json`");

    // test where 'value' is a map that contains a map
    test("select kvgen(p) from cp.`jsoninput/kvgen_complex_input.json`");
  }

  @Test
  // Test SplitUpComplexExpressions rule which splits complex expression into multiple projects
  public void testComplexAndSimpleColumnSelection() throws Exception {
    test("select t.a.b, kvgen(t.a.c) from cp.`jsoninput/input4.json` t");
  }

  @Test
  @Ignore( "until flattening code creates correct ListVector (DRILL-4045)" )
  public void testNestedFlatten() throws Exception {
    test("select flatten(rl) from cp.`jsoninput/input2.json`");
  }

  @Test //DRILL-2872.
  public void testRepeatedJson() throws Exception {

    final String query="select cast(convert_to(interests, 'JSON') as varchar(0)) as interests from cp.`complex_student.json`";
    testBuilder()
            .sqlQuery(query)
            .unOrdered()
            .jsonBaselineFile("DRILL-2872-result.json")
            .go();
  }

  @Test  // DRILL-4410
  // ListVector allocation
  public void test_array() throws Exception{

    long numRecords = 100000;

    String tempDir = BaseTestQuery.getTempDir("ComplexTypeWriter");
    String file1 = tempDir + TestComplexTypeReader.class.getName() + "arrays1.json";
    String file2 = tempDir + TestComplexTypeReader.class.getName() + "arrays2.json";
    Path path1 = Paths.get(file1);
    Path path2 = Paths.get(file2);

    String arrayString = "[ \"abcdef\", \"ghijkl\", \"mnopqr\", \"stuvwx\", \"yz1234\", \"567890\" ] ";
    Files.deleteIfExists(path1);
    Files.deleteIfExists(path2);
    Files.createFile(path1);
    Files.createFile(path2);

    try(PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(file1, true)))) {
      for (long i = 0; i < numRecords; i++) {
        out.println("{ \"id\" : " + i + ", \"array\" : " + arrayString + "}");
      }
    }catch (IOException e) {
      throw e;
    }

    try(PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(file2, true)))) {
      for (long i = 0; i < numRecords; i++) {
        out.println("{ \"id\" : " + i + ", \"array\" : " + arrayString + "}");
      }
    }catch (IOException e) {
      throw e;
    }

    String queryString = "select * from dfs.`" + file1 + "` `arrays1` INNER JOIN dfs.`" + file2 + "` `arrays2` ON "
            + "(`arrays1`.id = `arrays2`.id)";
    TestBuilder testBuilder = testBuilder().sqlQuery(queryString).unOrdered();
    testBuilder.baselineColumns("id", "id0", "array", "array0");
    for (long i = 0; i < numRecords; i++) {
      testBuilder.baselineValues(i, i, listOf("abcdef", "ghijkl", "mnopqr", "stuvwx", "yz1234", "567890"),
              listOf("abcdef", "ghijkl", "mnopqr", "stuvwx", "yz1234", "567890"));
    }
    testBuilder.go();

    Files.deleteIfExists(path1);
    Files.deleteIfExists(path2);
  }
}
