package org.apache.drill.exec.store.phoenix;

import static org.junit.Assert.fail;

import org.apache.drill.exec.physical.rowSet.RowSet;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.test.ClusterFixtureBuilder;
import org.apache.drill.test.ClusterTest;
import org.apache.drill.test.QueryRowSetIterator;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestPhoenixPlugin extends ClusterTest {

  private static final Logger logger = LoggerFactory.getLogger(TestPhoenixPlugin.class);

  @BeforeClass
  public static void setup() throws Exception {
    ClusterFixtureBuilder builder = new ClusterFixtureBuilder(dirTestWatcher);
    startCluster(builder);

    StoragePluginRegistry registry = cluster.drillbit().getContext().getStorage();
    PhoenixStoragePluginConfig config = new PhoenixStoragePluginConfig(null, "gwssi-app0102", 8765, "guest", "gwssi123", null);
    config.setEnabled(true);
    registry.put(PhoenixStoragePluginConfig.NAME, config);
  }

  public void test() {
    fail("Not yet implemented");
  }

  public void testSchema() throws Exception {
    String sql = "select * from phoenix.myTable";
    queryBuilder().sql(sql).run();
  }

  @Test
  public void testScan() throws Exception {
    String sql = "select * from phoenix.myTable";
    QueryRowSetIterator iterator = queryBuilder().sql(sql).rowSetIterator();
    int count = 0;
    for (RowSet rowset : iterator) {
      rowset.print();
      rowset.clear();
      count++;
    }
    logger.info("phoenix fetch batch size : {}", count + 1);
  }
}
