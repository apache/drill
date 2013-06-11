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
package org.apache.drill.common.logical.data;

import org.apache.drill.common.JSONOptions;
import org.apache.drill.common.defs.PartitionDef;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class StoreBuilderTest{

  /**
   * Build a Store operator and validate each field
   */
  @Test
  public void testBuild() {
    String storageEngine = "mock-storage";
    PartitionDef partition = new PartitionDef(PartitionDef.PartitionType.RANGE, null, null);
    JSONOptions target = null;

    Store storeOp = Store.builder()
        .storageEngine(storageEngine)
        .partition(partition)
        .target(target)
        .build();

    assertEquals(storeOp.getStorageEngine(), storageEngine);
    assertEquals(storeOp.getPartition(), partition);
    assertEquals(storeOp.getTarget(), target);
  }

}