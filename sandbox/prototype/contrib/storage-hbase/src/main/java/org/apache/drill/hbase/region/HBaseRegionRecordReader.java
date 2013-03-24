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
package org.apache.drill.hbase.region;

import org.apache.drill.exec.ref.RecordIterator;
import org.apache.drill.exec.ref.rse.RecordReader;

/**
 * An implementation of ReferenceStorageEngine for HBase.
 * <p/>
 * This implementation interfaces with HBase at the RegionServer level and allows per-region pushdown of selected operators
 * namely:
 * - Filter
 * - Project TODO
 * - Partial Aggregation TODO
 * - Local Join
 * <p/>
 * This implementation assumes that the daemon running this storage engine is colocated with the HBase RegionServer daemon.
 */
public class HBaseRegionRecordReader implements RecordReader {


  @Override
  public RecordIterator getIterator() {
    return null;
  }

  @Override
  public void setup() {
  }

  @Override
  public void cleanup() {
  }
}
