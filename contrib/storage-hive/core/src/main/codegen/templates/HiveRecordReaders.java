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
/*
 * This template is used to generate different Hive record reader classes for different data formats
 * to avoid JIT profile pullusion. These readers are derived from HiveAbstractReader which implements
 * codes for init and setup stage, but the repeated - and performance critical part - next() method is
 * separately implemented in the classes generated from this template. The internal SkipRecordReeader
 * class is also separated as well due to the same reason.
 *
 * As to the performance gain with this change, please refer to:
 * https://issues.apache.org/jira/browse/DRILL-4982
 *
 */
<@pp.dropOutputFile />
<#list hiveFormat.map as entry>
<@pp.changeOutputFile name="/org/apache/drill/exec/store/hive/Hive${entry.hiveReader}Reader.java" />
<#include "/@includes/license.ftl" />

package org.apache.drill.exec.store.hive.readers;

import java.util.Collection;
import java.util.List;
import java.util.Properties;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.store.hive.HivePartition;
import org.apache.drill.exec.store.hive.HiveTableWithColumnCache;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.vector.AllocationHelper;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.hive.conf.HiveConf;

import org.apache.hadoop.hive.serde2.SerDeException;

import org.apache.hadoop.mapred.RecordReader;

<#if entry.hasHeaderFooter == true>
import org.apache.drill.exec.store.hive.readers.inspectors.AbstractRecordsInspector;
import org.apache.drill.exec.store.hive.readers.inspectors.DefaultRecordsInspector;
import org.apache.drill.exec.store.hive.readers.inspectors.SkipFooterRecordsInspector;
import org.apache.drill.exec.store.hive.HiveUtilities;
import org.apache.hadoop.hive.serde.serdeConstants;
</#if>

public class Hive${entry.hiveReader}Reader extends HiveAbstractReader {

<#if entry.hasHeaderFooter == true>
  AbstractRecordsInspector recordsInspector;
<#else>
  Object value;
</#if>

  public Hive${entry.hiveReader}Reader(HiveTableWithColumnCache table, HivePartition partition, Collection<InputSplit> inputSplit, List<SchemaPath> projectedColumns,
                      FragmentContext context, final HiveConf hiveConf,
                      UserGroupInformation proxyUgi) throws ExecutionSetupException {
    super(table, partition, inputSplit, projectedColumns, context, hiveConf, proxyUgi);
  }

  public  void internalInit(Properties tableProperties, RecordReader<Object, Object> reader) {

    key = reader.createKey();
<#if entry.hasHeaderFooter == true>
    int skipHeaderCount = HiveUtilities.retrieveIntProperty(tableProperties, serdeConstants.HEADER_COUNT, -1);

    // skip first N records to apply skip header policy
    Object value = reader.createValue();
    for (int i = 0; i < skipHeaderCount; i++) {
      if (!hasNextValue(value)) {
        // no more records to skip, we drained the table
        empty = true;
        break;
      }
    }

    // if table was drained while skipping first N records, there is no need to check for skip footer logic
    if (!empty) {
      int skipFooterCount = HiveUtilities.retrieveIntProperty(tableProperties, serdeConstants.FOOTER_COUNT, -1);

      // if we need to skip N last records, use records inspector which will buffer records while reading
      if (skipFooterCount > 0) {
        recordsInspector = new SkipFooterRecordsInspector(reader, skipFooterCount);
      } else {
        recordsInspector = new DefaultRecordsInspector(reader.createValue());
      }
    }
<#else>
    value = reader.createValue();
</#if>

  }

<#if entry.hasHeaderFooter == true>

  @Override
  public int next() {
    for (ValueVector vv : vectors) {
      AllocationHelper.allocateNew(vv, TARGET_RECORD_COUNT);
    }

    if (empty) {
      setValueCountAndPopulatePartitionVectors(0);
      return 0;
    }

    try {
      // starting new batch, reset processed records count
      recordsInspector.reset();

      // process records till batch is full or all records were processed
      while (!recordsInspector.isBatchFull() && hasNextValue(recordsInspector.getValueHolder())) {
        Object value = recordsInspector.getNextValue();
        if (value != null) {
          Object deSerializedValue = partitionDeserializer.deserialize((Writable) value);
          if (partTblObjectInspectorConverter != null) {
            deSerializedValue = partTblObjectInspectorConverter.convert(deSerializedValue);
          }
          readHiveRecordAndInsertIntoRecordBatch(deSerializedValue, recordsInspector.getProcessedRecordCount());
          recordsInspector.incrementProcessedRecordCount();
        }
      }
      setValueCountAndPopulatePartitionVectors(recordsInspector.getProcessedRecordCount());
      return recordsInspector.getProcessedRecordCount();
    } catch (SerDeException e) {
      throw new DrillRuntimeException(e);
    }
  }

<#else>
  @Override
  public int next() {
    for (ValueVector vv : vectors) {
      AllocationHelper.allocateNew(vv, TARGET_RECORD_COUNT);
    }
    if (empty) {
      setValueCountAndPopulatePartitionVectors(0);
      return 0;
    }

    try {
      int recordCount = 0;
      while (recordCount < TARGET_RECORD_COUNT && hasNextValue(value)) {
        Object deSerializedValue = partitionDeserializer.deserialize((Writable) value);
        if (partTblObjectInspectorConverter != null) {
          deSerializedValue = partTblObjectInspectorConverter.convert(deSerializedValue);
        }
        readHiveRecordAndInsertIntoRecordBatch(deSerializedValue, recordCount);
        recordCount++;
      }

      setValueCountAndPopulatePartitionVectors(recordCount);
      return recordCount;
    } catch (SerDeException e) {
      throw new DrillRuntimeException(e);
    }
  }
</#if>

}
</#list>