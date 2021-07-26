
import com.google.common.collect.Lists;
import org.apache.drill.categories.RowSetTests;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.physical.rowSet.RowSet;
import org.apache.drill.exec.physical.rowSet.RowSetBuilder;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.store.fixedwidth.FixedwidthBatchReader;
import org.apache.drill.exec.store.fixedwidth.FixedwidthFieldConfig;
import org.apache.drill.exec.store.fixedwidth.FixedwidthFormatConfig;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.apache.drill.test.QueryBuilder;
import org.apache.drill.test.rowSet.RowSetComparison;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;

@Category(RowSetTests.class)
public class TestFixedwidthRecordReader extends ClusterTest {

//  @BeforeClass
//  public static void setup() throws Exception {
//    ClusterTest.startCluster(ClusterFixture.builder(dirTestWatcher));
//
//    // Needed for compressed file unit test
//    //dirTestWatcher.copyResourceToRoot(Paths.get("spss/"));
//  }

  @BeforeClass
  public static void setup() throws Exception {
    ClusterTest.startCluster(ClusterFixture.builder(dirTestWatcher));

    FixedwidthFormatConfig formatConfig = new FixedwidthFormatConfig(Lists.newArrayList("fwf")
            , Lists.newArrayList(
            new FixedwidthFieldConfig(TypeProtos.MinorType.INT, "Number", "", 1, 4),
            new FixedwidthFieldConfig(TypeProtos.MinorType.VARCHAR, "Letter", "", 6, 4),
            new FixedwidthFieldConfig(TypeProtos.MinorType.INT, "Address", "", 11, 3)
    ));
    cluster.defineFormat("cp", "fwf", formatConfig);
    //cluster.defineFormat("dfs", "xml", formatConfig);

    // Needed for compressed file unit test
    dirTestWatcher.copyResourceToRoot(Paths.get("fwf/"));
  }

  @Test
  public void testExplicitQuery() throws Exception {
    String sql = "SELECT ID, Urban, Urban_value FROM dfs.`spss/testdata.sav` WHERE d16=4";

    QueryBuilder q = client.queryBuilder().sql(sql);
    RowSet results = q.rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
            .addNullable("ID", TypeProtos.MinorType.FLOAT8)
            .addNullable("Urban", TypeProtos.MinorType.FLOAT8)
            .addNullable("Urban_value", TypeProtos.MinorType.VARCHAR)
            .buildSchema();


    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
            .addRow(47.0, 1.0, "Urban").addRow(53.0, 1.0, "Urban")
            .addRow(66.0, 1.0, "Urban")
            .build();

    assertEquals(3, results.rowCount());

    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testBatchReader() throws Exception {
    FixedwidthFieldConfig testField = new FixedwidthFieldConfig(TypeProtos.MinorType.FLOAT8, "date", "MM/DD/YYYY", 1, 10);
    System.out.println(testField.getFieldName());
    System.out.println(testField.getStartIndex());
    System.out.println(testField.getFieldWidth());
    System.out.println(testField.getDateTimeFormat());
    System.out.println(testField.getDataType());

    String sql = "SELECT * FROM cp.`fwf/test.fwf`";
    RowSet results = client.queryBuilder().sql(sql).rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
            .addNullable("Number", TypeProtos.MinorType.INT)
            .addNullable("Letter", TypeProtos.MinorType.VARCHAR)
            .addNullable("Address", TypeProtos.MinorType.INT)
            .buildSchema();


    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
            .addRow(1234, "test", 567)
            .build();

    assertEquals(1, results.rowCount());

    System.out.println(results.batchSchema());

    new RowSetComparison(expected).verifyAndClearAll(results);
    System.out.println("Test complete.");
    client.close();
  }




/*
BatchSchema [
fields=[
  [`Number` (INT:OPTIONAL),
      children=([`$bits$` (UINT1:REQUIRED)],
                [`Number` (INT:OPTIONAL)])],
  [`Letter` (VARCHAR:OPTIONAL),
      children=([`$bits$` (UINT1:REQUIRED)],
                [`Letter` (VARCHAR:OPTIONAL),
                      children=([`$offsets$` (UINT4:REQUIRED)])
                ]
               )
  ],
  [`Address` (INT:OPTIONAL),
      children=([`$bits$` (UINT1:REQUIRED)],
                [`Address` (INT:OPTIONAL)])
  ]
],
selectionVector=NONE]
*/

}