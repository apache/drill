package org.apache.drill.exec.store.cassandra;

import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.cassandra.CassandraStoragePluginConfig;
import org.apache.drill.test.ClusterFixtureBuilder;
import org.apache.drill.test.ClusterTest;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class CassandraQueryTest extends ClusterTest {

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
  public void test() throws Exception {
    String sql = "SELECT * FROM cassandra.drilltest.trending_now";
    queryBuilder().sql(sql).run();
  }
}
