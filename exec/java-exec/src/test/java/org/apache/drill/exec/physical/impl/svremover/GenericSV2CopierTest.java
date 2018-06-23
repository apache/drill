/*
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

import org.apache.drill.exec.memory.RootAllocator;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.test.rowSet.RowSet;
import org.apache.drill.test.rowSet.RowSetBuilder;
public class GenericSV2CopierTest extends AbstractGenericCopierTest {
  @Override
  public RowSet createSrcRowSet(RootAllocator allocator) {
    return new RowSetBuilder(allocator, createTestSchema(BatchSchema.SelectionVectorMode.TWO_BYTE))
      .addRow(row1())
      .addSelection(false, row4())
      .addRow(row2())
      .addSelection(false, row5())
      .addRow(row3())
      .withSv2()
      .build();
  }

  @Override
  public Copier createCopier() {
    return new GenericSV2Copier();
  }
}
