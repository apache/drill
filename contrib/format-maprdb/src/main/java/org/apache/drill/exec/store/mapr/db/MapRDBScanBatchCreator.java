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
package org.apache.drill.exec.store.mapr.db;

import java.util.List;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.ops.ExecutorFragmentContext;
import org.apache.drill.exec.physical.impl.BatchCreator;
import org.apache.drill.exec.physical.impl.ScanBatch;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.store.RecordReader;
import org.apache.drill.exec.store.hbase.HBaseRecordReader;
import org.apache.drill.exec.store.hbase.HBaseSubScan.HBaseSubScanSpec;
import org.apache.drill.exec.store.mapr.db.binary.BinaryTableGroupScan;
import org.apache.drill.exec.store.mapr.db.json.MaprDBJsonRecordReader;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class MapRDBScanBatchCreator implements BatchCreator<MapRDBSubScan> {
  @Override
  public ScanBatch getBatch(ExecutorFragmentContext context, MapRDBSubScan subScan, List<RecordBatch> children) throws ExecutionSetupException {
    Preconditions.checkArgument(children.isEmpty());
    List<RecordReader> readers = Lists.newArrayList();
    for(MapRDBSubScanSpec scanSpec : subScan.getRegionScanSpecList()){
      try {
        if (BinaryTableGroupScan.TABLE_BINARY.equals(subScan.getTableType())) {
          readers.add(new HBaseRecordReader(
              subScan.getFormatPlugin().getConnection(),
              getHBaseSubScanSpec(scanSpec),
              subScan.getColumns()));
        } else {
          readers.add(new MaprDBJsonRecordReader(scanSpec, subScan.getFormatPluginConfig(), subScan.getColumns(), context));
        }
      } catch (Exception e) {
        throw new ExecutionSetupException(e);
      }
    }
    return new ScanBatch(subScan, context, readers);
  }

  private HBaseSubScanSpec getHBaseSubScanSpec(MapRDBSubScanSpec scanSpec) {
    return new HBaseSubScanSpec(scanSpec.getTableName(), scanSpec.getRegionServer(),
        scanSpec.getStartRow(), scanSpec.getStopRow(), scanSpec.getSerializedFilter(), null);
  }

}
