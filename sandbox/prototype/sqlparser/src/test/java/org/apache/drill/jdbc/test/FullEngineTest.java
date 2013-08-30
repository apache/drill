package org.apache.drill.jdbc.test;

import java.io.IOException;

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;

@Ignore
public class FullEngineTest {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FullEngineTest.class);

  private static String MODEL_FULL_ENGINE;

  // Determine if we are in Eclipse Debug mode.
  static final boolean IS_DEBUG = java.lang.management.ManagementFactory.getRuntimeMXBean().getInputArguments().toString().indexOf("-agentlib:jdwp") > 0;

  // Set a timeout unless we're debugging.
  @Rule public TestRule globalTimeout = IS_DEBUG ? new TestName() : new Timeout(10000);
  
  @BeforeClass
  public static void setupFixtures() throws IOException {
    MODEL_FULL_ENGINE = Resources.toString(Resources.getResource("full-model.json"), Charsets.UTF_8);
  }
  
  @Test
  public void fullSelectStarEngine() throws Exception {
    JdbcAssert.withModel(MODEL_FULL_ENGINE, "DONUTS")
    // .sql("select cast(_MAP['red'] as bigint) + 1 as red_inc from donuts ")
        .sql("select _MAP['d'] as d, _MAP['b'] as b from \"parquet-local\".\"/tmp/parquet_test_file_many_types\" ").displayResults(50);
  }

//  @Test
//  public void fullEngine() throws Exception {
//    JdbcAssert
//        .withModel(MODEL_FULL_ENGINE, "DONUTS")
//        // .sql("select cast(_MAP['red'] as bigint) + 1 as red_inc from donuts ")
//        .sql(
//            "select cast(_MAP['RED'] as bigint)  as RED, cast(_MAP['GREEN'] as bigint)  as GREEN from 'monkeys' where cast(_MAP['red'] as BIGINT) > 1 ")
//        .displayResults(50);
//  }
}
