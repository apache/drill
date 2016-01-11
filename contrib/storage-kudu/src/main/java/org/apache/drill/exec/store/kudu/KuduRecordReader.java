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
package org.apache.drill.exec.store.kudu;

import io.netty.buffer.DrillBuf;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.store.kudu.KuduSubScan.KuduSubScanSpec;
import org.apache.drill.exec.vector.complex.impl.VectorContainerWriter;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.MapWriter;
import org.kududb.ColumnSchema;
import org.kududb.Type;
import org.kududb.client.KuduClient;
import org.kududb.client.KuduScanner;
import org.kududb.client.KuduTable;
import org.kududb.client.RowResult;
import org.kududb.client.RowResultIterator;
import org.kududb.client.shaded.com.google.common.collect.ImmutableMap;

public class KuduRecordReader extends AbstractRecordReader {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(KuduRecordReader.class);

  private static final int TARGET_RECORD_COUNT = 4000;

  private final KuduClient client;
  private final KuduSubScanSpec scanSpec;
  private KuduTable table;
  private VectorContainerWriter containerWriter;
  private MapWriter writer;
  private KuduScanner scanner;
  private RowResultIterator iterator;
  private DrillBuf buffer;

  public KuduRecordReader(KuduClient client, KuduSubScan.KuduSubScanSpec subScanSpec,
      List<SchemaPath> projectedColumns, FragmentContext context) {
    setColumns(projectedColumns);
    this.client = client;
    buffer = context.getManagedBuffer();
    scanSpec = subScanSpec;
  }

  @Override
  public void setup(OperatorContext context, OutputMutator output) throws ExecutionSetupException {
    try {
      KuduTable table = client.openTable(scanSpec.getTableName());
      scanner = client.newScannerBuilder(table).build();
      containerWriter = new VectorContainerWriter(output);
      writer = containerWriter.rootAsMap();
    } catch (Exception e) {
      throw new ExecutionSetupException(e);
    }
  }

  static final Map<Type, MajorType> TYPES;

  static {
    TYPES = ImmutableMap.<Type, MajorType> builder()
        .put(Type.BINARY, Types.optional(MinorType.VARBINARY))
        .put(Type.BOOL, Types.optional(MinorType.BIT))
        .put(Type.DOUBLE, Types.optional(MinorType.FLOAT8))
        .put(Type.FLOAT, Types.optional(MinorType.FLOAT4))
        .put(Type.INT16, Types.optional(MinorType.INT))
        .put(Type.INT32, Types.optional(MinorType.INT))
        .put(Type.INT8, Types.optional(MinorType.INT))
        .put(Type.INT64, Types.optional(MinorType.BIGINT))
        .put(Type.STRING, Types.optional(MinorType.VARCHAR))
        .put(Type.TIMESTAMP, Types.optional(MinorType.TIMESTAMP))
        .build();
  }

  @Override
  public int next() {
    int rowCount = 0;
    try {
      while (iterator == null || !iterator.hasNext()) {
        if (!scanner.hasMoreRows()) {
          iterator = null;
          return 0;
        }
        iterator = scanner.nextRows();
      }
      for (; rowCount < 4095 && iterator.hasNext(); rowCount++) {
        writer.setPosition(rowCount);
        addRowResult(iterator.next());
      }
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
    containerWriter.setValueCount(rowCount);
    return rowCount;
  }

  private void addRowResult(RowResult result) {
    int i = 0;
    for (ColumnSchema column : result.getColumnProjection().getColumns()) {
      switch (column.getType()) {
      case STRING: {
        final ByteBuffer buf = result.getBinary(i);
        final int length = buf.remaining();
        ensure(length);
        buffer.setBytes(0, buf);
        writer.varChar(column.getName()).writeVarChar(0, length, buffer);
        break;
      }
      case BINARY: {
        final ByteBuffer buf = result.getBinary(i);
        final int length = buf.remaining();
        ensure(length);
        buffer.setBytes(0, buf);
        writer.varBinary(column.getName()).writeVarBinary(0, length, buffer);
        break;
      }
      case INT8:
        writer.integer(column.getName()).writeInt(result.getByte(i));
        break;
      case INT16:
        writer.integer(column.getName()).writeInt(result.getShort(i));
        break;
      case INT32:
        writer.integer(column.getName()).writeInt(result.getInt(i));
        break;
      case INT64:
        writer.bigInt(column.getName()).writeBigInt(result.getLong(i));
        break;
      case FLOAT:
        writer.float4(column.getName()).writeFloat4(result.getFloat(i));
        break;
      case DOUBLE:
        writer.float8(column.getName()).writeFloat8(result.getDouble(i));
        break;
      case BOOL:
        writer.bit(column.getName()).writeBit(result.getBoolean(i) ? 1 : 0);
        break;
      case TIMESTAMP:
        writer.timeStamp(column.getName()).writeTimeStamp(result.getLong(i) / 1000);
        break;
      default:
        throw new UnsupportedOperationException("unsupported type " + column.getType());
      }

      i++;
    }
  }

  private void ensure(final int length) {
    buffer = buffer.reallocIfNeeded(length);
  }

  @Override
  public void close() {
  }

}
