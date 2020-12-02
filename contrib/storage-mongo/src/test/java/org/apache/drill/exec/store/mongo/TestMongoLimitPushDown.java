package org.apache.drill.exec.store.mongo;

import org.apache.drill.categories.MongoStorageTest;
import org.apache.drill.categories.SlowTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({SlowTest.class, MongoStorageTest.class})
public class TestMongoLimitPushDown extends MongoTestBase {


  @Test
  public void testLimitWithANDOperator() throws Exception {
    testBuilder()
      .sqlQuery(String.format(TEST_LIMIT_QUERY, EMPLOYEE_DB, EMPINFO_COLLECTION, 4))
      .unOrdered()
      .expectsNumRecords(4)
      .go();
  }
}
