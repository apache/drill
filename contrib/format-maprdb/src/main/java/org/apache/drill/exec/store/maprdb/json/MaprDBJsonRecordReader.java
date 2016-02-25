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

import static org.ojai.DocumentConstants.ID_KEY;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Stack;
import java.util.concurrent.TimeUnit;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.PathSegment;
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
import org.ojai.DocumentReader;
import org.ojai.DocumentReader.EventType;
import org.ojai.DocumentStream;
import org.ojai.FieldPath;
import org.ojai.FieldSegment;
import org.ojai.Value;
import org.ojai.store.QueryCondition;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
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

  private DocumentStream documentStream;

  private Iterator<DocumentReader> documentReaderIterators;

  private boolean includeId;
  private boolean idOnly;

  public MaprDBJsonRecordReader(MapRDBSubScanSpec subScanSpec,
      List<SchemaPath> projectedColumns, FragmentContext context) {
    buffer = context.getManagedBuffer();
    projectedFields = null;
    tableName = Preconditions.checkNotNull(subScanSpec, "MapRDB reader needs a sub-scan spec").getTableName();
    documentReaderIterators = null;
    includeId = false;
    idOnly    = false;
    condition = com.mapr.db.impl.ConditionImpl.parseFrom(ByteBufs.wrap(subScanSpec.getSerializedFilter()));
    setColumns(projectedColumns);
  }

  @Override
  protected Collection<SchemaPath> transformColumns(Collection<SchemaPath> columns) {
    Set<SchemaPath> transformed = Sets.newLinkedHashSet();
    if (!isStarQuery()) {
      Set<FieldPath> projectedFieldsSet = Sets.newTreeSet();
      for (SchemaPath column : columns) {
        if (column.getRootSegment().getPath().equalsIgnoreCase(ID_KEY)) {
          /*
           * we do not include _id field in the set of projected fields
           * because the DB currently can not return a document if only
           * the _id field was projected. This should really be fixed in
           * the DB client (Bug 21708) to avoid transferring the entire
           * document when only _id is requested.
           */
          // projectedFieldsList.add(ID_FIELD);
          includeId = true;
        } else {
          projectedFieldsSet.add(getFieldPathForProjection(column));
        }
        transformed.add(column);
      }
      if (projectedFieldsSet.size() > 0) {
        projectedFields = projectedFieldsSet.toArray(new FieldPath[projectedFieldsSet.size()]);
      }
    } else {
      transformed.add(AbstractRecordReader.STAR_COLUMN);
      includeId = true;
    }

    /*
     * (Bug 21708) if we are projecting only the id field, save that condition here.
     */
    idOnly = !isStarQuery() && (projectedFields == null);
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
    Stopwatch watch = Stopwatch.createUnstarted();
    watch.start();

    writer.allocate();
    writer.reset();

    int recordCount = 0;
    DBDocumentReaderBase reader = null;

    while(recordCount < BaseValueVector.INITIAL_VALUE_ALLOCATION) {
      try {
        reader = nextDocumentReader();
        if (reader == null) break;
        writer.setPosition(recordCount);
        if (idOnly) {
          Value id = reader.getId();
          MapWriter map = writer.rootAsMap();

          try {
            switch(id.getType()) {
            case STRING:
              writeString(map.varChar(ID_KEY), id.getString());
              recordCount++;
              break;
            case BINARY:
              writeBinary(map.varBinary(ID_KEY), id.getBinary());
              recordCount++;
              break;
            default:
              throw new UnsupportedOperationException(id.getType() +
                  " is not a supported type for _id field.");
            }
          } catch (IllegalStateException | IllegalArgumentException e) {
            logger.warn(String.format("Possible schema change at _id: '%s'",
                IdCodec.asString(id)), e);
          }
        } else {
          if (reader.next() != EventType.START_MAP) {
            throw dataReadError("The document did not start with START_MAP!");
          }
          writeToMap(reader, writer.rootAsMap());
          recordCount++;
        }
      } catch (UserException e) {
        throw UserException.unsupportedError(e)
            .addContext(String.format("Table: %s, document id: '%s'",
                table.getPath(),
                reader == null ? null : IdCodec.asString(reader.getId())))
            .build(logger);
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

      String fieldName = reader.getFieldName();
      try {
        switch (event) {
        case NULL:
          break; // not setting the field will leave it as null
        case BINARY:
          writeBinary(map.varBinary(fieldName), reader.getBinary());
          break;
        case BOOLEAN:
          map.bit(fieldName).writeBit(reader.getBoolean() ? 1 : 0);
          break;
        case STRING:
          writeString(map.varChar(fieldName), reader.getString());
          break;
        case BYTE:
          map.tinyInt(fieldName).writeTinyInt(reader.getByte());
          break;
        case SHORT:
          map.smallInt(fieldName).writeSmallInt(reader.getShort());
          break;
        case INT:
          map.integer(fieldName).writeInt(reader.getInt());
          break;
        case LONG:
          map.bigInt(fieldName).writeBigInt(reader.getLong());
          break;
        case FLOAT:
          map.float4(fieldName).writeFloat4(reader.getFloat());
          break;
        case DOUBLE:
          map.float8(fieldName).writeFloat8(reader.getDouble());
          break;
        case DECIMAL:
          throw unsupportedError("Decimal type is currently not supported.");
        case DATE:
          map.date(fieldName).writeDate(reader.getDate().toDate().getTime());
          break;
        case TIME:
          map.time(fieldName).writeTime(reader.getTimeInt());
          break;
        case TIMESTAMP:
          map.timeStamp(fieldName).writeTimeStamp(reader.getTimestampLong());
          break;
        case INTERVAL:
          throw unsupportedError("Interval type is currently not supported.");
        case START_MAP:
          writeToMap(reader, map.map(fieldName));
          break;
        case START_ARRAY:
          writeToList(reader, map.list(fieldName));
          break;
        case END_ARRAY:
          throw dataReadError("Encountered an END_ARRAY event inside a map.");
        default:
          throw unsupportedError("Unsupported type: %s encountered during the query.", event);
        }
      } catch (IllegalStateException | IllegalArgumentException e) {
        logger.warn(String.format("Possible schema change at _id: '%s', field: '%s'",
            IdCodec.asString(reader.getId()), fieldName), e);
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
        throw unsupportedError("Null values are not supported in lists.");
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
        throw unsupportedError("Decimals are currently not supported.");
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
        throw unsupportedError("Interval is currently not supported.");
      case START_MAP:
        writeToMap(reader, list.map());
        break;
      case END_MAP:
        throw dataReadError("Encountered an END_MAP event inside a list.");
      case START_ARRAY:
        writeToList(reader, list.list());
        break;
      default:
        throw unsupportedError("Unsupported type: %s encountered during the query.%s", event);
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

  private UserException unsupportedError(String format, Object... args) {
    return UserException.unsupportedError()
        .message(String.format(format, args))
        .build(logger);
  }

  private UserException dataReadError(String format, Object... args) {
    return UserException.dataReadError()
        .message(String.format(format, args))
        .build(logger);
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
      throw UserException.dataReadError(e).build(logger);
    }
  }

  /*
   * Extracts contiguous named segments from the SchemaPath, starting from the
   * root segment and build the FieldPath from it for projection.
   *
   * This is due to bug 22726 and 22727, which cause DB's DocumentReaders to
   * behave incorrectly for sparse lists, hence we avoid projecting beyond the
   * first encountered ARRAY field and let Drill handle the projection.
   */
  private static FieldPath getFieldPathForProjection(SchemaPath column) {
    Stack<PathSegment.NameSegment> pathSegments = new Stack<PathSegment.NameSegment>();
    PathSegment seg = column.getRootSegment();
    while (seg != null && seg.isNamed()) {
      pathSegments.push((PathSegment.NameSegment) seg);
      seg = seg.getChild();
    }
    FieldSegment.NameSegment child = null;
    while (!pathSegments.isEmpty()) {
      child = new FieldSegment.NameSegment(pathSegments.pop().getPath(), child, false);
    }
    return new FieldPath(child);
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
