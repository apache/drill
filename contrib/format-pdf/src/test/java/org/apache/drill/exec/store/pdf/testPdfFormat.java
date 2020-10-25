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
import org.apache.drill.test.rowSet.RowSetComparison;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.file.Paths;

public class testPdfFormat extends ClusterTest {

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
      " _creation_date, " +
      "_modification_date, " +
      "_trapped " +
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
      .buildSchema();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
      .addRow(1, "Agricultural Landuse Survey in The Sumas River Watershed Summa",
        "Vision", "Agricultural Landuse Survey in The Sumas River Watershed Summa",
        "Agricultural Landuse Survey in The Sumas River Watershed Summa",
        "PScript5.dll Version 5.2.2",
        "Acrobat Distiller 7.0.5 (Windows)",
        857403000000L,
        1230835135000L,
    null)
      .build();

    new RowSetComparison(expected).verifyAndClearAll(results);
  }

}
