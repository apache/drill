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
package org.apache.drill.exec.store.hive;

import java.io.IOException;
import java.util.List;

import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.vector.AllocationHelper;
import org.apache.drill.exec.vector.NullableBigIntVector;
import org.apache.drill.exec.vector.NullableIntVector;
import org.apache.drill.exec.vector.NullableVarCharVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputSplit;

import com.google.common.collect.Lists;

/**
 * Note: Native hive text record reader is not complete in implementation. For now use
 * {@link org.apache.drill.exec.store.hive.HiveRecordReader}.
 */
public class HiveTextRecordReader extends HiveRecordReader {

  public final byte delimiter;
  public final List<Integer> columnIds;
  private final int numCols;

  public HiveTextRecordReader(Table table, Partition partition, InputSplit inputSplit, List<SchemaPath> projectedColumns, FragmentContext context) throws ExecutionSetupException {
    super(table, partition, inputSplit, projectedColumns, context, null);
    String d = table.getSd().getSerdeInfo().getParameters().get("field.delim");
    if (d != null) {
      delimiter = d.getBytes()[0];
    } else {
      delimiter = (byte) 1;
    }
    assert delimiter > 0;
    List<Integer> ids = Lists.newArrayList();
    for (int i = 0; i < tableColumns.size(); i++) {
      if (selectedColumnNames.contains(tableColumns.get(i))) {
        ids.add(i);
      }
    }
    columnIds = ids;
    numCols = tableColumns.size();
  }

  public void setValue(PrimitiveObjectInspector.PrimitiveCategory pCat, ValueVector vv, int index, byte[] bytes, int start) {
    switch(pCat) {
      case BINARY:
        throw new UnsupportedOperationException();
      case BOOLEAN:
        throw new UnsupportedOperationException();
      case BYTE:
        throw new UnsupportedOperationException();
      case DECIMAL:
        throw new UnsupportedOperationException();
      case DOUBLE:
        throw new UnsupportedOperationException();
      case FLOAT:
        throw new UnsupportedOperationException();
      case INT: {
        int value = 0;
        byte b;
        for (int i = start; (b = bytes[i]) != delimiter; i++) {
          value = (value * 10) + b - 48;
        }
        ((NullableIntVector) vv).getMutator().setSafe(index, value);
      }
      case LONG: {
        long value = 0;
        byte b;
        for (int i = start; (b = bytes[i]) != delimiter; i++) {
          value = (value * 10) + b - 48;
        }
        ((NullableBigIntVector) vv).getMutator().setSafe(index, value);
      }
      case SHORT:
        throw new UnsupportedOperationException();
      case STRING: {
        int end = start;
        for (int i = start; i < bytes.length; i++) {
          if (bytes[i] == delimiter) {
            end = i;
            break;
          }
          end = bytes.length;
        }
        ((NullableVarCharVector) vv).getMutator().setSafe(index, bytes, start, end - start);
      }
      case TIMESTAMP:
        throw new UnsupportedOperationException();

      default:
        throw new UnsupportedOperationException("Could not determine type");
    }
  }


  @Override
  public int next() {
    for (ValueVector vv : vectors) {
      AllocationHelper.allocateNew(vv, TARGET_RECORD_COUNT);
    }
    try {
      int recordCount = 0;
      if (redoRecord != null) {
        int length = ((Text) value).getLength();
        byte[] bytes = ((Text) value).getBytes();
        int[] delimPositions = new int[numCols];
        delimPositions[0] = -1;
        int p = 0;
        for (int i = 0; i < length; i++) {
          if (bytes[i] == delimiter) {
            delimPositions[p++] = i;
          }
        }
        for (int id : columnIds) {
          boolean success = false; // setValue(primitiveCategories.get(id), vectors.get(id), recordCount, bytes, delimPositions[id]);
          if (!success) {
            throw new DrillRuntimeException(String.format("Failed to write value for column %s", selectedColumnNames.get(id)));
          }

        }
        redoRecord = null;
      }
      while (recordCount < TARGET_RECORD_COUNT && reader.next(key, value)) {
        int length = ((Text) value).getLength();
        byte[] bytes = ((Text) value).getBytes();
        int[] delimPositions = new int[numCols + 1];
        delimPositions[0] = -1;
        int p = 1;
        for (int i = 0; i < length; i++) {
          if (bytes[i] == delimiter) {
            delimPositions[p++] = i;
          }
        }
        for (int i = 0; i < columnIds.size(); i++) {
          int id = columnIds.get(i);
          boolean success = false; // setValue(primitiveCategories.get(i), vectors.get(i), recordCount, bytes, delimPositions[id] + 1);
          if (!success) {
            redoRecord = value;
            if (partition != null) {
              populatePartitionVectors(recordCount);
            }
            return recordCount;
          }
        }
        recordCount++;
      }
      if (partition != null) {
        populatePartitionVectors(recordCount);
      }
      return recordCount;
    } catch (IOException e) {
      throw new DrillRuntimeException(e);
    }
  }

}
