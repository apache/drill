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

package org.apache.drill.exec.store.pdf;

import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.rowSet.RowSet;
import org.apache.drill.exec.physical.rowSet.RowSetBuilder;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.apache.drill.test.QueryBuilder;
import org.apache.drill.test.QueryBuilder.QuerySummary;
import org.apache.drill.test.rowSet.RowSetComparison;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;

public class TestPdfFormat extends ClusterTest {

  @BeforeClass
  public static void setup() throws Exception {
    ClusterTest.startCluster(ClusterFixture.builder(dirTestWatcher));

    // Needed for compressed file unit test
    dirTestWatcher.copyResourceToRoot(Paths.get("pdf/"));
  }

  @Test
  public void testStarQuery() throws RpcException {
    String sql = "SELECT * FROM cp.`pdf/argentina_diputados_voting_record.pdf` WHERE `Provincia` = 'Rio Negro'";

    QueryBuilder q = client.queryBuilder().sql(sql);
    RowSet results = q.rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
      .addNullable("Apellido y Nombre", MinorType.VARCHAR)
      .addNullable("Bloque político", TypeProtos.MinorType.VARCHAR)
      .addNullable("Provincia", TypeProtos.MinorType.VARCHAR)
      .addNullable("field_0", TypeProtos.MinorType.VARCHAR)
      .buildSchema();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
      .addRow("ALBRIEU, Oscar Edmundo Nicolas", "Frente para la Victoria - PJ", "Rio Negro", "AFIRMATIVO")
      .addRow("AVOSCAN, Herman Horacio", "Frente para la Victoria - PJ", "Rio Negro", "AFIRMATIVO")
      .addRow("CEJAS, Jorge Alberto", "Frente para la Victoria - PJ", "Rio Negro", "AFIRMATIVO")
      .build();

    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testMetadataQuery() throws RpcException {
    String sql = "SELECT _page_count, " +
      "_title, " +
      "_author, " +
      "_subject, " +
      "_keywords, " +
      "_creator, " +
      "_producer," +
      "_creation_date, " +
      "_modification_date, " +
      "_trapped, " +
      "_table_count " +
      "FROM cp.`pdf/20.pdf` " +
      "LIMIT 1";

    QueryBuilder q = client.queryBuilder().sql(sql);
    RowSet results = q.rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
      .addNullable("_page_count", TypeProtos.MinorType.INT)
      .addNullable("_title", TypeProtos.MinorType.VARCHAR)
      .addNullable("_author", TypeProtos.MinorType.VARCHAR)
      .addNullable("_subject", TypeProtos.MinorType.VARCHAR)
      .addNullable("_keywords", TypeProtos.MinorType.VARCHAR)
      .addNullable("_creator", TypeProtos.MinorType.VARCHAR)
      .addNullable("_producer", MinorType.VARCHAR)
      .addNullable("_creation_date", MinorType.TIMESTAMP)
      .addNullable("_modification_date", MinorType.TIMESTAMP)
      .addNullable("_trapped", MinorType.VARCHAR)
      .addNullable("_table_count", MinorType.INT)
      .buildSchema();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
      .addRow(1, "Agricultural Landuse Survey in The Sumas River Watershed Summa",
        "Vision", "Agricultural Landuse Survey in The Sumas River Watershed Summa",
        "Agricultural Landuse Survey in The Sumas River Watershed Summa",
        "PScript5.dll Version 5.2.2",
        "Acrobat Distiller 7.0.5 (Windows)",
        857403000000L,
        1230835135000L,
        null, 1)
      .build();

    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testSerDe() throws Exception {
    String sql = "SELECT COUNT(*) AS cnt FROM " +
      "table(cp.`pdf/argentina_diputados_voting_record.pdf` (type => 'pdf', combinePages => false))";
    String plan = queryBuilder().sql(sql).explainJson();
    long cnt = queryBuilder().physical(plan).singletonLong();
    assertEquals("Counts should match",31L, cnt);
  }

  @Test
  public void testPageMerge() throws Exception {
    String sql = "SELECT * FROM cp.`pdf/schools.pdf`";
    QuerySummary results = client.queryBuilder().sql(sql).run();
    assertEquals(271, results.recordCount());
  }
}