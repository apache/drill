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

import com.google.common.collect.Lists;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.drill.TestBuilder;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.hive.HiveTestBase;
import org.junit.Test;

import java.util.List;

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
  public void testXpath_Double() throws Exception {
    final String query = "select xpath_double ('<a><b>20</b><c>40</c></a>', 'a/b * a/c') as col \n" +
        "from hive.kv \n" +
        "limit 0";

    final TypeProtos.MajorType majorType = TypeProtos.MajorType.newBuilder()
        .setMinorType(TypeProtos.MinorType.FLOAT8)
        .setMode(TypeProtos.DataMode.OPTIONAL)
        .build();

    final List<Pair<SchemaPath, TypeProtos.MajorType>> expectedSchema = Lists.newArrayList();
    expectedSchema.add(Pair.of(SchemaPath.getSimplePath("col"), majorType));

    testBuilder()
        .sqlQuery(query)
        .schemaBaseLine(expectedSchema)
        .build()
        .run();
  }

  @Test // DRILL-4459
  public void testGetJsonObject() throws Exception {
    testBuilder()
        .sqlQuery("select convert_from(json, 'json') as json from hive.simple_json " +
            "where GET_JSON_OBJECT(simple_json.json, '$.employee_id') like 'Emp2'")
        .ordered()
        .baselineColumns("json")
        .baselineValues(TestBuilder.mapOf("employee_id","Emp2","full_name","Kamesh",
            "first_name","Bh","last_name","Venkata","position","Store"))
        .go();
  }

  @Test // DRILL-3272
  public void testIf() throws Exception {
    testBuilder()
        .sqlQuery("select `if`(1999 > 2000, 'latest', 'old') Period from hive.kv limit 1")
        .ordered()
        .baselineColumns("Period")
        .baselineValues("old")
        .go();
  }

}
