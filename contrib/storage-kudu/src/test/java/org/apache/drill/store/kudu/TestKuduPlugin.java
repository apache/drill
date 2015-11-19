package org.apache.drill.store.kudu;

import org.apache.drill.BaseTestQuery;
import org.junit.Test;

public class TestKuduPlugin extends BaseTestQuery {

  @Test
  public void testBasicQuery() throws Exception {
    test("select * from kudu.demo;");
  }
}
