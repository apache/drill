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
import org.apache.drill.common.expression.FieldReference;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ScanBuilderTest{

  /**
   * Build a Scan operator and validate each field
   */
  @Test
  public void testBuild() {
    FieldReference outputFieldReference = new FieldReference("Flavour");
    JSONOptions selection = null;
    String storageEngine = "mock-storage";

    Scan scanOp = Scan.builder()
        .storageEngine(storageEngine)
        .outputReference(outputFieldReference)
        .selection(selection)
        .build();

    assertEquals(scanOp.getOutputReference(), outputFieldReference);
    assertEquals(scanOp.getSelection(), selection);
    assertEquals(scanOp.getStorageEngine(), storageEngine);
  }

}