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
package org.apache.drill.exec.sql;

import org.apache.drill.BaseTestQuery;
import org.junit.Test;

public class TestViewSupport extends BaseTestQuery{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestViewSupport.class);

  @Test
  public void referToSchemaInsideAndOutsideView() throws Exception {
    String use = "use dfs_test.tmp;";
    String selectInto = "create table monkey as select c_custkey, c_regionkey from cp.`tpch/customer.parquet`";
    String createView = "create or replace view myMonkeyView as select c_custkey, c_regionkey from monkey";
    String selectInside = "select * from myMonkeyView;";
    String use2 = "use cp;";
    String selectOutside = "select * from dfs_test.tmp.myMonkeyView;";

    test(use);
    test(selectInto);
    test(createView);
    test(selectInside);
    test(use2);
    test(selectOutside);
  }


}
