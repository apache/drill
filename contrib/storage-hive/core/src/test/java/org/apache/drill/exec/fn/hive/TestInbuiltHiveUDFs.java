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

import org.apache.drill.exec.hive.HiveTestBase;
import org.junit.Test;

public class TestInbuiltHiveUDFs extends HiveTestBase {

  @Test // DRILL-3273
  public void testConcatWS() throws Exception {
    testBuilder()
        .sqlQuery("SELECT concat_ws(string_field, string_part, '|') as rst from hive.readtest")
        .unOrdered()
        .baselineColumns("rst")
        .baselineValues("stringstringfield|")
        .baselineValues(new Object[] { null })
        .go();
  }

  @Test // DRILL-3273
  public void testEncode() throws Exception {
    testBuilder()
        .sqlQuery("SELECT encode(varchar_field, 'UTF-8') as rst from hive.readtest")
        .unOrdered()
        .baselineColumns("rst")
        .baselineValues("varcharfield".getBytes())
        .baselineValues(new Object[] { null })
        .go();
  }

  @Test
  public void testReflect() throws Exception {
    final String query = "select reflect('java.lang.Math', 'round', cast(2 as float)) as col \n" +
        "from hive.kv \n" +
        "limit 1";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("col")
        .baselineValues("2")
        .go();
  }

  @Test
  public void testAbs() throws Exception {
    final String query = "select reflect('java.lang.Math', 'abs', cast(-2 as double)) as col \n" +
        "from hive.kv \n" +
        "limit 1";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("col")
        .baselineValues("2.0")
        .go();
  }
}
