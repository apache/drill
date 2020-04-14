package org.apache.drill.exec.store.json;

import org.apache.drill.exec.ExecConstants;
import org.junit.Test;

public class TestJsonReaderWithSchema extends BaseTestJsonReader {

  @Test
  public void testSelectFromListWithCase() throws Exception {
    try {
      testBuilder()
              .sqlQuery("select a, typeOf(a) `type` from " +
                "(select case when is_list(field2) then field2[4][1].inner7 end a " +
                "from cp.`jsoninput/union/a.json`) where a is not null")
              .ordered()
              .optionSettingQueriesForTestQuery("alter session set `exec.enable_union_type` = true")
              .baselineColumns("a", "type")
              .baselineValues(13L, "BIGINT")
              .go();
    } finally {
      client.resetSession(ExecConstants.ENABLE_UNION_TYPE_KEY);
    }
  }
}
