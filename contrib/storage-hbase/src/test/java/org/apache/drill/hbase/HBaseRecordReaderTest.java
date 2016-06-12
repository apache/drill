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

import org.junit.Test;

public class HBaseRecordReaderTest extends BaseHBaseTest {

  @Test
  public void testLocalDistributed() throws Exception {
    String planName = "/hbase/hbase_scan_screen_physical.json";
    runHBasePhysicalVerifyCount(planName, HBaseTestsSuite.TEST_TABLE_1.getNameAsString(), 8);
  }

  @Test
  public void testLocalDistributedColumnSelect() throws Exception {
    String planName = "/hbase/hbase_scan_screen_physical_column_select.json";
    runHBasePhysicalVerifyCount(planName, HBaseTestsSuite.TEST_TABLE_1.getNameAsString(), 3);
  }

  @Test
  public void testLocalDistributedFamilySelect() throws Exception {
    String planName = "/hbase/hbase_scan_screen_physical_family_select.json";
    runHBasePhysicalVerifyCount(planName, HBaseTestsSuite.TEST_TABLE_1.getNameAsString(), 4);
  }

}
