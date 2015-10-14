/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store.solr;

import org.apache.drill.BaseTestQuery;
import org.apache.drill.exec.store.solr.SolrScanSpec;
import org.junit.Before;
import org.junit.Test;

public class TestSolr extends BaseTestQuery {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory
      .getLogger(TestSolr.class);
  private static final String solrServer = "http://localhost:20000/solr/";
  private static final String solrCoreName = "bootstrap_5";
  private SolrScanSpec solrScanSpec;

  @Before
  public void setUp() {
    solrScanSpec = new SolrScanSpec(solrCoreName);

  }

  @Test
  public void solrBasicQuery() throws Exception {
    testBuilder().sqlQuery("select * from solr.`bootstrap_5`").unOrdered()
        .build().run();
  }

  @Test
  public void solrFilterQuery() throws Exception {
    testBuilder()
        .sqlQuery(
            "select * from solr.`bikemini_3` where start_station_id='514'")
        .unOrdered().build().run();
  }
}
