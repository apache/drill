/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.drill.exec.store.httpd;

import java.util.List;
import org.apache.drill.BaseTestQuery;
import org.apache.drill.exec.rpc.user.QueryDataBatch;
import static org.junit.Assert.*;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestHttpdLogFormatPlugin extends BaseTestQuery {

  private static final Logger LOG = LoggerFactory.getLogger(TestHttpdLogFormatPlugin.class);

  /**
   * This test covers the test bootstrap-storage-plugins.json section of httpd.
   *
   * Indirectly this validates the HttpdLogFormatPlugin.HttpdLogFormatConfig deserializing properly.
   *
   * @throws Exception
   */
  @Test
  public void testDfsTestBootstrap_star() throws Exception {
    test("select * from dfs_test.`${WORKING_PATH}/src/test/resources/store/httpd/dfs-test-bootstrap-test.httpd`");
  }

  /**
   * This test covers the test bootstrap-storage-plugins.json section of httpd.
   *
   * Indirectly this validates the HttpdLogFormatPlugin.HttpdLogFormatConfig deserializing properly.
   *
   * @throws Exception
   */
  @Test
  public void testDfsTestBootstrap_notstar() throws Exception {
    test("select `TIME_STAMP:request_receive_time`, `HTTP_METHOD:request_firstline_method`, `STRING:request_status_last`, `BYTES:response_body_bytesclf` \n"
        + "from dfs_test.`${WORKING_PATH}/src/test/resources/store/httpd/dfs-test-bootstrap-test.httpd`");
  }

  /**
   * This test covers the main bootstrap-storage-plugins.json section of httpd.
   *
   * @throws Exception
   */
  @Test
  public void testDfsBootstrap_star() throws Exception {
    test("select * from dfs.`${WORKING_PATH}/src/test/resources/store/httpd/dfs-bootstrap.httpd`");
  }

  /**
   * This test covers the main bootstrap-storage-plugins.json section of httpd.
   *
   * @throws Exception
   */
  @Test
  public void testDfsBootstrap_wildcard() throws Exception {
    test("select `STRING:request_referer_query_$` from dfs.`${WORKING_PATH}/src/test/resources/store/httpd/dfs-bootstrap.httpd`");
  }

  /**
   * This test covers the main bootstrap-storage-plugins.json section of httpd.
   *
   * @throws Exception
   */
  @Test
  public void testDfsBootstrap_underscore() throws Exception {
    test("select `TIME_DAY:request_receive_time_day__utc` from dfs.`${WORKING_PATH}/src/test/resources/store/httpd/dfs-bootstrap.httpd`");
  }

  @Test
  public void testGroupBy_1() throws Exception {
    final List<QueryDataBatch> actualResults = testSqlWithResults(
        "select `HTTP_METHOD:request_firstline_method` as http_method, `STRING:request_status_last` as status_code, sum(`BYTES:response_body_bytesclf`) as total_bytes \n"
        + "from dfs_test.`${WORKING_PATH}/src/test/resources/store/httpd/dfs-test-bootstrap-test.httpd`\n"
        + "group by `HTTP_METHOD:request_firstline_method`, `STRING:request_status_last`"
    );

    final TestResultSet expectedResultSet = new TestResultSet();
    expectedResultSet.addRow("GET", "200", "46551");
    expectedResultSet.addRow("POST", "302", "18186");

    TestResultSet actualResultSet = new TestResultSet(actualResults);
    assertTrue(expectedResultSet.equals(actualResultSet));
  }
}
