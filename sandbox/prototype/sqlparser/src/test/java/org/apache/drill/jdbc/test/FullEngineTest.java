package org.apache.drill.jdbc.test;

import java.io.IOException;

import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;

public class FullEngineTest {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FullEngineTest.class);

  private static String MODEL_FULL_ENGINE;

  @BeforeClass
  public static void setupFixtures() throws IOException {
    MODEL_FULL_ENGINE = Resources.toString(Resources.getResource("full-model.json"), Charsets.UTF_8);
  }
  
  @Test
  public void fullSelectStarEngine() throws Exception {
    JdbcAssert.withModel(MODEL_FULL_ENGINE, "DONUTS")
    // .sql("select cast(_MAP['red'] as bigint) + 1 as red_inc from donuts ")
        .sql("select * from donuts ").displayResults(50);
  }

  @Test
  public void fullEngine() throws Exception {
    JdbcAssert
        .withModel(MODEL_FULL_ENGINE, "DONUTS")
        // .sql("select cast(_MAP['red'] as bigint) + 1 as red_inc from donuts ")
        .sql(
            "select cast(_MAP['RED'] as bigint)  as RED, cast(_MAP['GREEN'] as bigint)  as GREEN from donuts where cast(_MAP['red'] as BIGINT) > 1 ")
        .displayResults(50);
  }
}
