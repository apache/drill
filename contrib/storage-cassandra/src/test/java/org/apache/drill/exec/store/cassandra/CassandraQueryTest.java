package org.apache.drill.exec.store.cassandra;

import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.physical.rowSet.RowSet;
import org.apache.drill.exec.physical.rowSet.RowSetBuilder;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.test.ClusterFixtureBuilder;
import org.apache.drill.test.ClusterTest;
import org.apache.drill.test.QueryBuilder;
import org.apache.drill.test.rowSet.RowSetComparison;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class CassandraQueryTest extends ClusterTest {

  private static final Logger logger = LoggerFactory.getLogger(CassandraQueryTest.class);

  @BeforeClass
  public static void setup() throws Exception {
    ClusterFixtureBuilder builder = new ClusterFixtureBuilder(dirTestWatcher);
    startCluster(builder);

    List<String> hosts = new ArrayList<>();
    hosts.add("127.0.0.1");

    StoragePluginRegistry pluginRegistry = cluster.drillbit().getContext().getStorage();
    CassandraStoragePluginConfig config = new CassandraStoragePluginConfig(hosts, 9042);
    config.setEnabled(true);
    pluginRegistry.createOrUpdate(CassandraStoragePluginConfig.NAME, config, true);
  }

  @Test
  public void testSimpleStarQuery() throws Exception {
    String sql = "SELECT * FROM cassandra.drilltest.trending_now";
    QueryBuilder q = queryBuilder().sql(sql);
    RowSet results = q.rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
      .addNullable("id", TypeProtos.MinorType.VARCHAR)
      .addNullable("pog_rank", TypeProtos.MinorType.VARCHAR)
      .addNullable("pog_id", TypeProtos.MinorType.VARCHAR)
      .buildSchema();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
      .addRow("id0001", "1", "10001")
      .addRow("id0005", "1", "10001")
      .addRow("id0002", "1", "10001")
      .addRow("id0002", "2", "10001")
      .addRow("id0002", "3", "10001")
      .addRow("id0006", "1", "10001")
      .addRow("id0006", "2", "10001")
      .addRow("id0004", "1", "10001")
      .addRow("id0004", "2", "10001")
      .addRow("id0004", "3", "10002")
      .addRow("id0004", "4", "10002")
      .addRow("id0004", "5", "10002")
      .addRow("id0004", "6", "10002")
      .addRow("id0003", "1", "10001")
      .build();

    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testExplicitFields() throws Exception {
    String sql = "SELECT `id`, `pog_id` FROM cassandra.drilltest.trending_now";
    QueryBuilder q = queryBuilder().sql(sql);
    RowSet results = q.rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
      .addNullable("id", TypeProtos.MinorType.VARCHAR)
      .addNullable("pog_id", TypeProtos.MinorType.VARCHAR)
      .buildSchema();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
      .addRow("id0001", "10001")
      .addRow("id0005", "10001")
      .addRow("id0002", "10001")
      .addRow("id0002", "10001")
      .addRow("id0002", "10001")
      .addRow("id0006", "10001")
      .addRow("id0006", "10001")
      .addRow("id0004", "10001")
      .addRow("id0004", "10001")
      .addRow("id0004", "10002")
      .addRow("id0004", "10002")
      .addRow("id0004", "10002")
      .addRow("id0004", "10002")
      .addRow("id0003", "10001")
      .build();

    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testEqualToFilter() throws Exception {
    String sql = "SELECT `id` FROM cassandra.drilltest.trending_now WHERE pog_rank = 5";
    QueryBuilder q = queryBuilder().sql(sql);
    RowSet results = q.rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
      .addNullable("id", TypeProtos.MinorType.VARCHAR)
      .buildSchema();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
      .addRow("id0004")
      .addRow("id0004")
      .build();

    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testGreaterThanOrEqualTo() throws Exception {
    String sql = "SELECT `id` FROM cassandra.drilltest.trending_now WHERE pog_rank >= 5";
    QueryBuilder q = queryBuilder().sql(sql);
    RowSet results = q.rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
      .addNullable("id", TypeProtos.MinorType.VARCHAR)
      .buildSchema();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
      .addRow("id0004")
      .addRow("id0004")
      .build();

    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testSerDe() throws Exception {
    String sql = "SELECT COUNT(*) as cnt FROM cassandra.drilltest.trending_now";
    String plan = queryBuilder().sql(sql).explainJson();
    logger.debug("Query plan: {}", plan);

    long cnt = queryBuilder().physical(plan).singletonLong();

    assertEquals("Counts should match",1L, cnt);
  }
}
