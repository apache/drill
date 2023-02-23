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

package org.apache.drill.exec.expr.fn.impl;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.rowSet.RowSet;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.apache.drill.test.rowSet.RowSetComparison;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.drill.test.rowSet.RowSetUtilities.strArray;

public class TestRegexpFunctions extends ClusterTest {

  @BeforeClass
  public static void setup() throws Exception {
    ClusterTest.startCluster(ClusterFixture.builder(dirTestWatcher));
  }

  @Test
  public void testRegexpExtraction() throws Exception {
    String sql = "SELECT regexp_extract('123-456-789', '([0-9]{3})-([0-9]{3})-([0-9]{3})') AS extractedText, " +
        "regexp_extract('123-456-789', '[0-9]{3}-[0-9]{3}-[0-9]{3}') AS none";
    RowSet results = client.queryBuilder().sql(sql).rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
        .add("extractedText", MinorType.VARCHAR, DataMode.REPEATED)
        .add("none", MinorType.VARCHAR, DataMode.REPEATED)
        .buildSchema();

    RowSet expected = client.rowSetBuilder(expectedSchema)
        .addRow(strArray("123", "456", "789"), strArray())
        .build();

    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testRegexpExtractionWithIndex() throws Exception {
    String sql = "SELECT regexp_extract('123-456-789', '([0-9]{3})-([0-9]{3})-([0-9]{3})', 1) AS extractedText, " +
        "regexp_extract('123-456-789', '([0-9]{3})-([0-9]{3})-([0-9]{3})', 0) AS allText";
    RowSet results = client.queryBuilder().sql(sql).rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
        .add("extractedText", MinorType.VARCHAR)
        .add("allText", MinorType.VARCHAR)
        .buildSchema();

    RowSet expected = client.rowSetBuilder(expectedSchema)
        .addRow("123", "123-456-789")
        .build();

    new RowSetComparison(expected).verifyAndClearAll(results);
  }
}
