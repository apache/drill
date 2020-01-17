package org.apache.drill.exec.store.cassandra;

import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.test.ClusterFixtureBuilder;
import org.apache.drill.test.ClusterTest;
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
    queryBuilder().sql(sql).run();
  }

  @Test
  public void testExplicitFields() throws Exception {
    String sql = "SELECT `id`, `pog_id` FROM cassandra.drilltest.trending_now";
    queryBuilder().sql(sql).run();
  }

  @Test
  public void testSerDe() throws Exception {
    String sql = "SELECT COUNT(*) as cnt FROM cassandra.drilltest.trending_now";
    String plan = queryBuilder().sql(sql).explainJson();
    logger.debug("Query plan: {}", plan);

    long count = queryBuilder().physical(plan).singletonLong();

    assertEquals("Counts should match",14L, count);
  }
}
