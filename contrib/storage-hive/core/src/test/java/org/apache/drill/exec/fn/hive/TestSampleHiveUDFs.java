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
package org.apache.drill.exec.fn.hive;

import static org.junit.Assert.assertTrue;

import java.util.List;

import org.apache.drill.BaseTestQuery;
import org.apache.drill.exec.rpc.user.QueryResultBatch;
import org.apache.drill.exec.store.hive.HiveTestDataGenerator;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

public class TestSampleHiveUDFs extends BaseTestQuery {

  @BeforeClass
  public static void addStoragePlugins() throws Exception{
    new HiveTestDataGenerator().createAndAddHiveTestPlugin(bit.getContext().getStorage());
  }

  private void helper(String query, String expected) throws Exception {
    List<QueryResultBatch> results = testSqlWithResults(query);
    String actual = getResultString(results, ",");
    assertTrue(String.format("Result:\n%s\ndoes not match:\n%s", actual, expected), expected.equals(actual));
  }

  @Test
  public void booleanInOut() throws Exception{
    String query = "SELECT " +
        "testHiveUDFBoolean(true) as col1," +
        "testHiveUDFBoolean(false) as col2," +
        "testHiveUDFBoolean(cast(null as boolean)) as col3 " +
        "FROM hive.kv LIMIT 1";

    String expected = "col1,col2,col3\n" + "true,false,null\n";
    helper(query, expected);
  }

  @Test
  public void byteInOut() throws Exception{
    String query = "SELECT testHiveUDFByte(tinyint_field) as col1 FROM hive.readtest";
    String expected = "col1\n" + "34\n" + "null\n";
    helper(query, expected);
  }

  @Test
  public void shortInOut() throws Exception{
    String query = "SELECT testHiveUDFShort(smallint_field) as col1 FROM hive.readtest";
    String expected = "col1\n" + "3455\n" + "null\n";
    helper(query, expected);
  }

  @Test
  public void intInOut() throws Exception{
    String query = "SELECT testHiveUDFInt(int_field) as col1 FROM hive.readtest";
    String expected = "col1\n" + "123456\n" + "null\n";
    helper(query, expected);
  }

  @Test
  public void longInOut() throws Exception{
    String query = "SELECT testHiveUDFLong(bigint_field) as col1 FROM hive.readtest";
    String expected = "col1\n" + "234235\n" + "null\n";
    helper(query, expected);
  }

  @Test
  public void floatInOut() throws Exception{
    String query = "SELECT testHiveUDFFloat(float_field) as col1 FROM hive.readtest";
    String expected = "col1\n" + "4.67\n" + "null\n";    helper(query, expected);
  }

  @Test
  public void doubleInOut() throws Exception{
    String query = "SELECT testHiveUDFDouble(double_field) as col1 FROM hive.readtest";
    String expected = "col1\n" + "8.345\n" + "null\n";
    helper(query, expected);
  }

  @Test
  public void stringInOut() throws Exception{
    String query = "SELECT testHiveUDFString(string_field) as col1 FROM hive.readtest";
    String expected = "col1\n" + "stringfield\n" + "\n";
    helper(query, expected);
  }

  @Test
  public void binaryInOut() throws Exception{
    String query = "SELECT testHiveUDFBinary(binary_field) as col1 FROM hive.readtest";
    String expected = "col1\n" + "binaryfield\n" + "\n";
    helper(query, expected);    helper(query, expected);
  }

  @Test
  public void varcharInOut() throws Exception{
    String query = "SELECT " +
        "testHiveUDFVarChar('This is a varchar') as col1," +
        "testHiveUDFVarChar(cast(null as varchar)) as col2 " +
        "FROM hive.kv LIMIT 1";

    String expected = "col1,col2\n" + "This is a varchar,null\n";
    helper(query, expected);
  }

  @Test
  @Ignore("doesn't work across timezones")
  public void dateInOut() throws Exception{
    String query = "SELECT " +
        "testHiveUDFDate(cast('1970-01-02 10:20:33' as date)) as col1," +
        "testHiveUDFDate(cast(null as date)) as col2 " +
        "FROM hive.kv LIMIT 1";

    String expected = "col1,col2\n" + "1970-01-01T08:00:00.000-08:00,null\n";
    helper(query, expected);
  }

  @Test
  @Ignore("doesn't work across timezones")
  public void timestampInOut() throws Exception{
    String query = "SELECT " +
        "testHiveUDFTimeStamp(cast('1970-01-02 10:20:33' as timestamp)) as col1," +
        "testHiveUDFTimeStamp(cast(null as timestamp)) as col2 " +
        "FROM hive.kv LIMIT 1";

    String expected = "col1,col2\n" + "1970-01-02T10:20:33.000-08:00,null\n";
    helper(query, expected);
  }

  @Test
  public void decimalInOut() throws Exception{
    String query = "SELECT " +
        "testHiveUDFDecimal(cast('1234567891234567891234567891234567891.4' as decimal(38, 1))) as col1 " +
        "FROM hive.kv LIMIT 1";

    String expected = "col1\n" + "1234567891234567891234567891234567891.4\n";
    helper(query, expected);
  }
}
