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
package org.apache.drill.exec.store.mock;

import java.util.LinkedList;
import java.util.List;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.ops.ExecutorFragmentContext;
import org.apache.drill.exec.physical.impl.BatchCreator;
import org.apache.drill.exec.physical.impl.ScanBatch;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.store.RecordReader;

import org.apache.drill.exec.store.mock.MockTableDef.MockScanEntry;

import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;

public class MockScanBatchCreator implements BatchCreator<MockSubScanPOP> {
  @Override
  public ScanBatch getBatch(ExecutorFragmentContext context, MockSubScanPOP config, List<RecordBatch> children)
      throws ExecutionSetupException {
    Preconditions.checkArgument(children.isEmpty());
    final List<MockScanEntry> entries = config.getReadEntries();
    final List<RecordReader> readers = new LinkedList<>();
    for(final MockTableDef.MockScanEntry e : entries) {
      if ( e.isExtended( ) ) {
        readers.add(new ExtendedMockRecordReader(e));
      } else {
        readers.add(new MockRecordReader(context, e));
      }
    }
    return new ScanBatch(config, context, readers);
  }
}
