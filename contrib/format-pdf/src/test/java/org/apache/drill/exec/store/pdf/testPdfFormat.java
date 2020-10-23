package org.apache.drill.exec.store.pdf;

import org.apache.drill.exec.physical.rowSet.RowSet;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.apache.drill.test.QueryBuilder;
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
    String sql = "SELECT * FROM cp.`pdf/argentina_diputados_voting_record.pdf`";

    QueryBuilder q = client.queryBuilder().sql(sql);
    RowSet results = q.rowSet();
    results.print();

    /*TupleMetadata expectedSchema = new SchemaBuilder()
      .addNullable("id", TypeProtos.MinorType.FLOAT8)
      .addNullable("first_name", TypeProtos.MinorType.VARCHAR)
      .addNullable("last_name", TypeProtos.MinorType.VARCHAR)
      .addNullable("email", TypeProtos.MinorType.VARCHAR)
      .addNullable("gender", TypeProtos.MinorType.VARCHAR)
      .addNullable("birthdate", TypeProtos.MinorType.VARCHAR)
      .addNullable("balance", TypeProtos.MinorType.FLOAT8)
      .addNullable("order_count", TypeProtos.MinorType.FLOAT8)
      .addNullable("average_order", TypeProtos.MinorType.FLOAT8)
      .buildSchema();*/

    /*RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
      .addRow(1.0, "Cornelia", "Matej", "cmatej0@mtv.com", "Female", "10/31/1974", 735.29, 22.0, 33.42227273)
      .addRow(2.0, "Nydia", "Heintsch", "nheintsch1@godaddy.com", "Female", "12/10/1966", 784.14, 22.0, 35.64272727)
      .addRow(3.0, "Waiter", "Sherel", "wsherel2@utexas.edu", "Male", "3/12/1961", 172.36, 17.0, 10.13882353)
      .addRow(4.0, "Cicely", "Lyver", "clyver3@mysql.com", "Female", "5/4/2000", 987.39, 6.0, 164.565)
      .addRow(5.0, "Dorie", "Doe", "ddoe4@spotify.com", "Female", "12/28/1955", 852.48, 17.0, 50.14588235)
      .build();*/

    //new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testExplicitMetadataQuery() throws RpcException {
    String sql = "SELECT _page_count, _author FROM cp.`pdf/campaign_donors.pdf`";

    QueryBuilder q = client.queryBuilder().sql(sql);
    RowSet results = q.rowSet();
    results.print();

    /*TupleMetadata expectedSchema = new SchemaBuilder()
      .addNullable("id", TypeProtos.MinorType.FLOAT8)
      .addNullable("first_name", TypeProtos.MinorType.VARCHAR)
      .addNullable("last_name", TypeProtos.MinorType.VARCHAR)
      .addNullable("email", TypeProtos.MinorType.VARCHAR)
      .addNullable("gender", TypeProtos.MinorType.VARCHAR)
      .addNullable("birthdate", TypeProtos.MinorType.VARCHAR)
      .addNullable("balance", TypeProtos.MinorType.FLOAT8)
      .addNullable("order_count", TypeProtos.MinorType.FLOAT8)
      .addNullable("average_order", TypeProtos.MinorType.FLOAT8)
      .buildSchema();*/

    /*RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
      .addRow(1.0, "Cornelia", "Matej", "cmatej0@mtv.com", "Female", "10/31/1974", 735.29, 22.0, 33.42227273)
      .addRow(2.0, "Nydia", "Heintsch", "nheintsch1@godaddy.com", "Female", "12/10/1966", 784.14, 22.0, 35.64272727)
      .addRow(3.0, "Waiter", "Sherel", "wsherel2@utexas.edu", "Male", "3/12/1961", 172.36, 17.0, 10.13882353)
      .addRow(4.0, "Cicely", "Lyver", "clyver3@mysql.com", "Female", "5/4/2000", 987.39, 6.0, 164.565)
      .addRow(5.0, "Dorie", "Doe", "ddoe4@spotify.com", "Female", "12/28/1955", 852.48, 17.0, 50.14588235)
      .build();*/

    //new RowSetComparison(expected).verifyAndClearAll(results);
  }

}
