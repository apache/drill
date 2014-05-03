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
package org.apache.drill.hbase;

import org.junit.Ignore;
import org.junit.Test;

@Ignore // Need to find a way to pass zookeeper port to HBase storage plugin configuration before enabling this test
public class TestHBaseProjectPushDown extends BaseHBaseTest {

  @Test
  public void testRowKeyPushDown() throws Exception{
    verify("SELECT\n"
        + "row_key, substring(row_key, 2, 1)*12\n"
        + "FROM\n"
        + "  hbase.`[TABLE_NAME]` tableName"
        , 6);
  }

}
