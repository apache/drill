/*******************************************************************************
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
 ******************************************************************************/
package org.apache.drill.exec.ref;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.logical.StorageEngineConfig;
import org.apache.drill.exec.ref.rse.ConsoleRSE;
import org.apache.drill.exec.ref.rse.RSERegistry;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;

public class RSERegistryTest {

  @Test
  public void testEnginesWithTheSameNameAreEqual() {
    DrillConfig config = DrillConfig.create();
    RSERegistry rses = new RSERegistry(config);
    StorageEngineConfig hconfig = new ConsoleRSE.ConsoleRSEConfig("console");
    ConsoleRSE engine = (ConsoleRSE) rses.getEngine(hconfig);
    ConsoleRSE engine2 = (ConsoleRSE) rses.getEngine(hconfig);
    assertEquals(engine, engine2);
  }
}
