package org.apache.drill.exec.store.splunk;

import org.apache.drill.categories.SlowTest;
import org.apache.drill.exec.physical.rowSet.RowSet;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runners.MethodSorters;

import static org.junit.Assert.assertEquals;

@FixMethodOrder(MethodSorters.JVM)
@Category({SlowTest.class})
public class SplunkWriterTest extends SplunkBaseTest {

  @Test
  public void testBasicCTAS() throws Exception {

    // Verify that there is no index called t1 in Splunk
    String sql = "SELECT * FROM INFORMATION_SCHEMA.`TABLES` WHERE TABLE_SCHEMA = 'splunk' AND TABLE_NAME LIKE 't1'";
    RowSet results = client.queryBuilder().sql(sql).rowSet();
    assertEquals(0, results.rowCount());
    results.clear();

    // Now create the table
    sql = "CREATE TABLE `splunk`.`t1` AS SELECT * FROM cp.`test_data.csvh`";
    results = client.queryBuilder().sql(sql).rowSet();
    assertEquals(1, results.rowCount());
    results.clear();

    // Verify that an index was created called t1 in Splunk
    sql = "SELECT * FROM INFORMATION_SCHEMA.`TABLES` WHERE TABLE_SCHEMA = 'splunk' AND TABLE_NAME LIKE 't1'";
    results = client.queryBuilder().sql(sql).rowSet();
    assertEquals(1, results.rowCount());
    results.clear();

    // Next verify that the results arrived.
    sql = "SELECT * FROM splunk.`t1`";
    results = client.queryBuilder().sql(sql).rowSet();
    results.print();
    results.clear();
  }
}
