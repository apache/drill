/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.hive;

import org.apache.drill.PlanTestBase;
import org.apache.drill.exec.store.hive.HiveTestDataGenerator;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/**
 * Base class for Hive test. Takes care of generating and adding Hive test plugin before tests and deleting the
 * plugin after tests.
 */
public class HiveTestBase extends PlanTestBase {
  protected static HiveTestDataGenerator hiveTest;

  @BeforeClass
  public static void generateHive() throws Exception{
    hiveTest = HiveTestDataGenerator.getInstance();
    hiveTest.addHiveTestPlugin(getDrillbitContext().getStorage());
  }

  @AfterClass
  public static void cleanupHiveTestData() throws Exception{
    if (hiveTest != null) {
      hiveTest.deleteHiveTestPlugin(getDrillbitContext().getStorage());
    }
  }
}
