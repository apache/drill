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
package org.apache.drill.exec.physical.impl.svremover;

import static org.junit.Assert.assertEquals;

import java.util.Properties;

import org.apache.drill.BaseTestQuery;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.ExecConstants;
import org.junit.Test;

public class TestSVRemover extends BaseTestQuery {
  @Test
  public void testSelectionVectorRemoval() throws Exception {
    int numOutputRecords = testPhysical(getFile("remover/test1.json"));
    assertEquals(50, numOutputRecords);
  }

  @Test
  public void testSVRWithNoFilter() throws Exception {
    int numOutputRecords = testPhysical(getFile("remover/sv_with_no_filter.json"));
    assertEquals(100, numOutputRecords);
  }

  /**
   * Test the generic version of the selection vector remover copier
   * class. The code uses the traditional generated version by default.
   * This test sets the option to use the generic version, then runs
   * a query that exercises that version.
   * <p>
   * Note that the tests here exercise only the SV2 version of the
   * selection remover; no tests exist for the SV4 version.
   */

  // TODO: Add an SV4 test once the improved mock data generator
  // is available.

  @Test
  public void testGenericCopier() throws Exception {
    // TODO: replace this with new setup once revised test framework
    // is available.
    Properties config = new Properties( );
    config.put(ExecConstants.SYS_STORE_PROVIDER_LOCAL_ENABLE_WRITE, "false");
    config.put(ExecConstants.HTTP_ENABLE, "false");
    config.put(ExecConstants.REMOVER_ENABLE_GENERIC_COPIER, "true");
    updateTestCluster(1, DrillConfig.create(config));

    int numOutputRecords = testPhysical(getFile("remover/test1.json"));
    assertEquals(50, numOutputRecords);
    numOutputRecords = testPhysical(getFile("remover/sv_with_no_filter.json"));
    assertEquals(100, numOutputRecords);
  }
}
