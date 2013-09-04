package org.apache.drill.jdbc.test;

import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;


public class FullEngineTest {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FullEngineTest.class);

  // Determine if we are in Eclipse Debug mode.
  static final boolean IS_DEBUG = java.lang.management.ManagementFactory.getRuntimeMXBean().getInputArguments().toString().indexOf("-agentlib:jdwp") > 0;

  // Set a timeout unless we're debugging.
  @Rule public TestRule globalTimeout = IS_DEBUG ? new TestName() : new Timeout(10000);
  
  @Test
  @Ignore // since this is a specifically located file.
  public void fullSelectStarEngine() throws Exception {
    JdbcAssert.withFull("parquet-local")
    // .sql("select cast(_MAP['red'] as bigint) + 1 as red_inc from donuts ")
        .sql("select _MAP['d'] as d, _MAP['b'] as b from \"/tmp/parquet_test_file_many_types\" ").displayResults(50);
  }

  @Test
  public void setCPRead() throws Exception {
    JdbcAssert.withFull("json-cp")
    // .sql("select cast(_MAP['red'] as bigint) + 1 as red_inc from donuts ")
        .sql("select * from \"department.json\" ").displayResults(50);
  }

}
