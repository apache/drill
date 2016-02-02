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
package org.apache.drill.exec.store.maprdb.json;

import static org.ojai.DocumentConstants.ID_FIELD;
import static org.ojai.DocumentConstants.ID_KEY;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.ops.OperatorStats;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.store.maprdb.MapRDBSubScanSpec;
import org.apache.drill.exec.vector.BaseValueVector;
import org.apache.drill.exec.vector.complex.impl.VectorContainerWriter;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.ListWriter;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.MapWriter;
import org.apache.drill.exec.vector.complex.writer.VarBinaryWriter;
import org.apache.drill.exec.vector.complex.writer.VarCharWriter;
import org.ojai.Document;
import org.ojai.DocumentReader;
import org.ojai.DocumentReader.EventType;
import org.ojai.DocumentStream;
import org.ojai.FieldPath;
import org.ojai.store.QueryCondition;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.mapr.db.MapRDB;
import com.mapr.db.Table;
import com.mapr.db.Table.TableOption;
import com.mapr.db.exceptions.DBException;
import com.mapr.db.impl.IdCodec;
import com.mapr.db.ojai.DBDocumentReaderBase;
import com.mapr.db.util.ByteBufs;
import com.mapr.org.apache.hadoop.hbase.util.Bytes;

import io.netty.buffer.DrillBuf;

public class MaprDBJsonRecordReader extends AbstractRecordReader {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MaprDBJsonRecordReader.class);

  public static final SchemaPath ID_PATH = SchemaPath.getSimplePath(ID_KEY);

  private Table table;
  private QueryCondition condition;
  private FieldPath[] projectedFields;

  private String tableName;
  private OperatorContext operatorContext;
  private VectorContainerWriter writer;

  private DrillBuf buffer;

  private DocumentStream<Document> documentStream;

  private Iterator<DocumentReader> documentReaderIterators;

  private boolean includeId;

  private String currentFieldName;

  public MaprDBJsonRecordReader(MapRDBSubScanSpec subScanSpec,
      List<SchemaPath> projectedColumns, FragmentContext context) {
    buffer = context.getManagedBuffer();
    tableName = Preconditions.checkNotNull(subScanSpec, "MapRDB reader needs a sub-scan spec").getTableName();
    includeId = false;
    condition = com.mapr.db.impl.ConditionImpl.parseFrom(ByteBufs.wrap(subScanSpec.getSerializedFilter()));
    setColumns(projectedColumns);
  }

  @Override
  protected Collection<SchemaPath> transformColumns(Collection<SchemaPath> columns) {
    Set<SchemaPath> transformed = Sets.newLinkedHashSet();
    if (!isStarQuery()) {
      ArrayList<Object> projectedFieldsList = Lists.newArrayList();
      for (SchemaPath column : columns) {
        if (column.getRootSegment().getPath().equalsIgnoreCase(ID_KEY)) {
          transformed.add(ID_PATH);
          projectedFieldsList.add(ID_FIELD);
          includeId = true;
        } else {
          transformed.add(SchemaPath.getSimplePath(column.getRootSegment().getPath()));
          projectedFieldsList.add(FieldPath.parseFrom(column.getAsUnescapedPath()));
        }
      }
      projectedFields = projectedFieldsList.toArray(new FieldPath[projectedFieldsList.size()]);
    } else {
      transformed.add(ID_PATH);
      includeId = true;
    }

    return transformed;
  }

  @Override
  public void setup(OperatorContext context, OutputMutator output) throws ExecutionSetupException {
    this.writer = new VectorContainerWriter(output);
    this.operatorContext = context;

    try {
      table = MapRDB.getTable(tableName);
      table.setOption(TableOption.EXCLUDEID, !includeId);
      documentStream = table.find(condition, projectedFields);
      documentReaderIterators = documentStream.documentReaders().iterator();
    } catch (DBException e) {
      throw new ExecutionSetupException(e);
    }
  }

  @Override
  public int next() {
    Stopwatch watch = new Stopwatch();
    watch.start();

    writer.allocate();
    writer.reset();

    int recordCount = 0;

    while(recordCount < BaseValueVector.INITIAL_VALUE_ALLOCATION) {
      DBDocumentReaderBase reader = nextDocumentReader();
      if (reader == null) break;
      writer.setPosition(recordCount);
      if (reader.next() != EventType.START_MAP) {
        throw new IllegalStateException("The document did not start with START_MAP!");
      }
      try {
        writeToMap(reader, writer.rootAsMap());
        recordCount++;
      } catch (IllegalArgumentException e) {
        logger.warn(String.format("Possible schema change at _id: '%s', field: '%s'",
            IdCodec.asString(reader.getId()), currentFieldName), e);
      }
    }

    writer.setValueCount(recordCount);
    logger.debug("Took {} ms to get {} records", watch.elapsed(TimeUnit.MILLISECONDS), recordCount);
    return recordCount;
  }

  private void writeToMap(DBDocumentReaderBase reader, MapWriter map) {
    map.start();
    outside: while (true) {
      EventType event = reader.next();
      if (event == null || event == EventType.END_MAP) break outside;

      currentFieldName = reader.getFieldName();
      switch (event) {
      case NULL:
        map.varChar(currentFieldName).write(null); // treat as VARCHAR for now
      case BINARY:
        writeBinary(map.varBinary(currentFieldName), reader.getBinary());
        break;
      case BOOLEAN:
        map.bit(currentFieldName).writeBit(reader.getBoolean() ? 1 : 0);
        break;
      case STRING:
        writeString(map.varChar(currentFieldName), reader.getString());
        break;
      case BYTE:
        map.tinyInt(currentFieldName).writeTinyInt(reader.getByte());
        break;
      case SHORT:
        map.smallInt(currentFieldName).writeSmallInt(reader.getShort());
        break;
      case INT:
        map.integer(currentFieldName).writeInt(reader.getInt());
        break;
      case LONG:
        map.bigInt(currentFieldName).writeBigInt(reader.getLong());
        break;
      case FLOAT:
        map.float4(currentFieldName).writeFloat4(reader.getFloat());
        break;
      case DOUBLE:
        map.float8(currentFieldName).writeFloat8(reader.getDouble());
        break;
      case DECIMAL:
        throw new UnsupportedOperationException("Decimals are currently not supported.");
      case DATE:
        map.date(currentFieldName).writeDate(reader.getDate().toDate().getTime());
        break;
      case TIME:
        map.time(currentFieldName).writeTime(reader.getTimeInt());
        break;
      case TIMESTAMP:
        map.timeStamp(currentFieldName).writeTimeStamp(reader.getTimestampLong());
        break;
      case INTERVAL:
        throw new UnsupportedOperationException("Interval is currently not supported.");
      case START_MAP:
        writeToMap(reader, map.map(currentFieldName));
        break;
      case START_ARRAY:
        writeToList(reader, map.list(currentFieldName));
        break;
      case END_ARRAY:
        throw new IllegalStateException("Shouldn't get a END_ARRAY inside a map");
      default:
        throw new UnsupportedOperationException("Unsupported type: " + event);
      }
    }
    map.end();
  }

  private void writeToList(DBDocumentReaderBase reader, ListWriter list) {
    list.startList();
    outside: while (true) {
      EventType event = reader.next();
      if (event == null || event == EventType.END_ARRAY) break outside;

      switch (event) {
      case NULL:
        list.varChar().write(null); // treat as VARCHAR for now
      case BINARY:
        writeBinary(list.varBinary(), reader.getBinary());
        break;
      case BOOLEAN:
        list.bit().writeBit(reader.getBoolean() ? 1 : 0);
        break;
      case STRING:
        writeString(list.varChar(), reader.getString());
        break;
      case BYTE:
        list.tinyInt().writeTinyInt(reader.getByte());
        break;
      case SHORT:
        list.smallInt().writeSmallInt(reader.getShort());
        break;
      case INT:
        list.integer().writeInt(reader.getInt());
        break;
      case LONG:
        list.bigInt().writeBigInt(reader.getLong());
        break;
      case FLOAT:
        list.float4().writeFloat4(reader.getFloat());
        break;
      case DOUBLE:
        list.float8().writeFloat8(reader.getDouble());
        break;
      case DECIMAL:
        throw new UnsupportedOperationException("Decimals are currently not supported.");
      case DATE:
        list.date().writeDate(reader.getDate().toDate().getTime());
        break;
      case TIME:
        list.time().writeTime(reader.getTimeInt());
        break;
      case TIMESTAMP:
        list.timeStamp().writeTimeStamp(reader.getTimestampLong());
        break;
      case INTERVAL:
        throw new UnsupportedOperationException("Interval is currently not supported.");
      case START_MAP:
        writeToMap(reader, list.map());
        break;
      case END_MAP:
        throw new IllegalStateException("Shouldn't get a END_MAP inside a list");
      case START_ARRAY:
        writeToList(reader, list.list());
        break;
      default:
        throw new UnsupportedOperationException("Unsupported type: " + event);
      }
    }
    list.endList();
  }

  private void writeBinary(VarBinaryWriter binaryWriter, ByteBuffer buf) {
    buffer = buffer.reallocIfNeeded(buf.remaining());
    buffer.setBytes(0, buf, buf.position(), buf.remaining());
    binaryWriter.writeVarBinary(0, buf.remaining(), buffer);
  }

  private void writeString(VarCharWriter varCharWriter, String string) {
    final byte[] strBytes = Bytes.toBytes(string);
    buffer = buffer.reallocIfNeeded(strBytes.length);
    buffer.setBytes(0, strBytes);
    varCharWriter.writeVarChar(0, strBytes.length, buffer);
  }

  private DBDocumentReaderBase nextDocumentReader() {
    final OperatorStats operatorStats = operatorContext == null ? null : operatorContext.getStats();
    try {
      if (operatorStats != null) {
        operatorStats.startWait();
      }
      try {
        if (!documentReaderIterators.hasNext()) {
          return null;
        } else {
          return (DBDocumentReaderBase) documentReaderIterators.next();
        }
      } finally {
        if (operatorStats != null) {
          operatorStats.stopWait();
        }
      }
    } catch (DBException e) {
      throw new DrillRuntimeException(e);
    }
  }

  @Override
  public void close() {
    if (documentStream != null) {
      documentStream.close();
    }
    if (table != null) {
      table.close();
    }
  }

}
