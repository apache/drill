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
package org.apache.drill.exec.store.mapr.db.json;

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
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.ops.OperatorStats;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.proto.UserBitShared.DrillPBError.ErrorType;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.store.mapr.db.MapRDBFormatPluginConfig;
import org.apache.drill.exec.store.mapr.db.MapRDBSubScanSpec;
import org.apache.drill.exec.vector.BaseValueVector;
import org.apache.drill.exec.vector.complex.impl.MapOrListWriterImpl;
import org.apache.drill.exec.vector.complex.impl.VectorContainerWriter;
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
  private final long MILLISECONDS_IN_A_DAY  = (long)1000 * 60 * 60 * 24;

  private Table table;
  private QueryCondition condition;
  private FieldPath[] projectedFields;

  private final String tableName;
  private OperatorContext operatorContext;
  private VectorContainerWriter vectorWriter;

  private DrillBuf buffer;

  private DocumentStream documentStream;

  private Iterator<DocumentReader> documentReaderIterators;

  private boolean includeId;
  private boolean idOnly;
  private final boolean unionEnabled;
  private final boolean readNumbersAsDouble;
  private boolean disablePushdown;
  private final boolean allTextMode;
  private final boolean ignoreSchemaChange;

  public MaprDBJsonRecordReader(MapRDBSubScanSpec subScanSpec,
      MapRDBFormatPluginConfig formatPluginConfig,
      List<SchemaPath> projectedColumns, FragmentContext context) {
    buffer = context.getManagedBuffer();
    projectedFields = null;
    tableName = Preconditions.checkNotNull(subScanSpec, "MapRDB reader needs a sub-scan spec").getTableName();
    documentReaderIterators = null;
    includeId = false;
    idOnly    = false;
    byte[] serializedFilter = subScanSpec.getSerializedFilter();
    condition = null;

    if (serializedFilter != null) {
      condition = com.mapr.db.impl.ConditionImpl.parseFrom(ByteBufs.wrap(serializedFilter));
    }

    setColumns(projectedColumns);
    unionEnabled = context.getOptions().getOption(ExecConstants.ENABLE_UNION_TYPE);
    readNumbersAsDouble = formatPluginConfig.isReadAllNumbersAsDouble();
    allTextMode = formatPluginConfig.isAllTextMode();
    ignoreSchemaChange = formatPluginConfig.isIgnoreSchemaChange();
    disablePushdown = !formatPluginConfig.isEnablePushdown();
  }

  @Override
  protected Collection<SchemaPath> transformColumns(Collection<SchemaPath> columns) {
    Set<SchemaPath> transformed = Sets.newLinkedHashSet();
    if (!isStarQuery() && !disablePushdown) {
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
    this.vectorWriter = new VectorContainerWriter(output, unionEnabled);
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

    vectorWriter.allocate();
    vectorWriter.reset();

    int recordCount = 0;
    DBDocumentReaderBase reader = null;

    while(recordCount < BaseValueVector.INITIAL_VALUE_ALLOCATION) {
      vectorWriter.setPosition(recordCount);
      try {
        reader = nextDocumentReader();
        if (reader == null) {
          break; // no more documents for this scanner
        } else if (isSkipQuery()) {
          vectorWriter.rootAsMap().bit("count").writeBit(1);
        } else {
          MapOrListWriterImpl writer = new MapOrListWriterImpl(vectorWriter.rootAsMap());
          if (idOnly) {
            writeId(writer, reader.getId());
          } else {
            if (reader.next() != EventType.START_MAP) {
              throw dataReadError("The document did not start with START_MAP!");
            }
            writeToListOrMap(writer, reader);
          }
        }
        recordCount++;
      } catch (UserException e) {
        throw UserException.unsupportedError(e)
            .addContext(String.format("Table: %s, document id: '%s'",
                table.getPath(),
                reader == null ? null : IdCodec.asString(reader.getId())))
            .build(logger);
      } catch (SchemaChangeException e) {
        if (ignoreSchemaChange) {
          logger.warn("{}. Dropping the row from result.", e.getMessage());
          logger.debug("Stack trace:", e);
        } else {
          throw dataReadError(e);
        }
      }
    }

    vectorWriter.setValueCount(recordCount);
    logger.debug("Took {} ms to get {} records", watch.elapsed(TimeUnit.MILLISECONDS), recordCount);
    return recordCount;
  }

  private void writeId(MapOrListWriterImpl writer, Value id) throws SchemaChangeException {
    try {
      switch(id.getType()) {
      case STRING:
        writeString(writer, ID_KEY, id.getString());
        break;
      case BINARY:
        writeBinary(writer, ID_KEY, id.getBinary());
        break;
      default:
        throw new UnsupportedOperationException(id.getType() +
            " is not a supported type for _id field.");
      }
    } catch (IllegalStateException | IllegalArgumentException e) {
      throw schemaChangeException(e, "Possible schema change at _id: '%s'", IdCodec.asString(id));
    }
  }

  private void writeToListOrMap(MapOrListWriterImpl writer, DBDocumentReaderBase reader) throws SchemaChangeException {
    String fieldName = null;
    writer.start();
    outside: while (true) {
      EventType event = reader.next();
      if (event == null
          || event == EventType.END_MAP
          || event == EventType.END_ARRAY) {
        break outside;
      } else if (reader.inMap()) {
        fieldName = reader.getFieldName();
      }

      try {
        switch (event) {
        case NULL:
          break; // not setting the field will leave it as null
        case BINARY:
          writeBinary(writer, fieldName, reader.getBinary());
          break;
        case BOOLEAN:
          writeBoolean(writer, fieldName, reader);
          break;
        case STRING:
          writeString(writer, fieldName, reader.getString());
          break;
        case BYTE:
          writeByte(writer, fieldName, reader);
          break;
        case SHORT:
          writeShort(writer, fieldName, reader);
          break;
        case INT:
          writeInt(writer, fieldName, reader);
          break;
        case LONG:
          writeLong(writer, fieldName, reader);
          break;
        case FLOAT:
          writeFloat(writer, fieldName, reader);
          break;
        case DOUBLE:
          writeDouble(writer, fieldName, reader);
          break;
        case DECIMAL:
          throw unsupportedError("Decimal type is currently not supported.");
        case DATE:
          writeDate(writer, fieldName, reader);
          break;
        case TIME:
          writeTime(writer, fieldName, reader);
          break;
        case TIMESTAMP:
          writeTimeStamp(writer, fieldName, reader);
          break;
        case INTERVAL:
          throw unsupportedError("Interval type is currently not supported.");
        case START_MAP:
          writeToListOrMap((MapOrListWriterImpl) (reader.inMap() ? writer.map(fieldName) : writer.listoftmap(fieldName)), reader);
          break;
        case START_ARRAY:
          writeToListOrMap((MapOrListWriterImpl) writer.list(fieldName), reader);
          break;
        default:
          throw unsupportedError("Unsupported type: %s encountered during the query.", event);
        }
      } catch (IllegalStateException | IllegalArgumentException e) {
        throw schemaChangeException(e, "Possible schema change at _id: '%s', field: '%s'", IdCodec.asString(reader.getId()), fieldName);
      }
    }
    writer.end();
  }

  private void writeTimeStamp(MapOrListWriterImpl writer, String fieldName, DBDocumentReaderBase reader) {
    if (allTextMode) {
      writeString(writer, fieldName, reader.getTimestamp().toUTCString());
    } else {
      ((writer.map != null) ? writer.map.timeStamp(fieldName) : writer.list.timeStamp()).writeTimeStamp(reader.getTimestampLong());
    }
  }

  private void writeTime(MapOrListWriterImpl writer, String fieldName, DBDocumentReaderBase reader) {
    if (allTextMode) {
      writeString(writer, reader.getTime().toTimeStr(), fieldName);
    } else {
      ((writer.map != null) ? writer.map.time(fieldName) : writer.list.time()).writeTime(reader.getTimeInt());
    }
  }

  private void writeDate(MapOrListWriterImpl writer, String fieldName, DBDocumentReaderBase reader) {
    if (allTextMode) {
      writeString(writer, reader.getDate().toDateStr(), fieldName);
    } else {
      long milliSecondsSinceEpoch = reader.getDateInt() * MILLISECONDS_IN_A_DAY;
      ((writer.map != null) ? writer.map.date(fieldName) : writer.list.date()).writeDate(milliSecondsSinceEpoch);
    }
  }

  private void writeDouble(MapOrListWriterImpl writer, String fieldName, DBDocumentReaderBase reader) {
    if (allTextMode) {
      writeString(writer, String.valueOf(reader.getDouble()), fieldName);
    } else {
      writer.float8(fieldName).writeFloat8(reader.getDouble());
    }
  }

  private void writeFloat(MapOrListWriterImpl writer, String fieldName, DBDocumentReaderBase reader) {
    if (allTextMode) {
      writeString(writer, String.valueOf(reader.getFloat()), fieldName);
    } else if (readNumbersAsDouble) {
      writer.float8(fieldName).writeFloat8(reader.getFloat());
    } else {
      writer.float4(fieldName).writeFloat4(reader.getFloat());
    }
  }

  private void writeLong(MapOrListWriterImpl writer, String fieldName, DBDocumentReaderBase reader) {
    if (allTextMode) {
      writeString(writer, String.valueOf(reader.getLong()), fieldName);
    } else if (readNumbersAsDouble) {
      writer.float8(fieldName).writeFloat8(reader.getLong());
    } else {
      writer.bigInt(fieldName).writeBigInt(reader.getLong());
    }
  }

  private void writeInt(MapOrListWriterImpl writer, String fieldName, DBDocumentReaderBase reader) {
    if (allTextMode) {
      writeString(writer, String.valueOf(reader.getInt()), fieldName);
    } else if (readNumbersAsDouble) {
      writer.float8(fieldName).writeFloat8(reader.getInt());
    } else {
      writer.integer(fieldName).writeInt(reader.getInt());
    }
  }

  private void writeShort(MapOrListWriterImpl writer, String fieldName, DBDocumentReaderBase reader) {
    if (allTextMode) {
      writeString(writer, String.valueOf(reader.getShort()), fieldName);
    } else if (readNumbersAsDouble) {
      writer.float8(fieldName).writeFloat8(reader.getShort());
    } else {
      ((writer.map != null) ? writer.map.smallInt(fieldName) : writer.list.smallInt()).writeSmallInt(reader.getShort());
    }
  }

  private void writeByte(MapOrListWriterImpl writer, String fieldName, DBDocumentReaderBase reader) {
    if (allTextMode) {
      writeString(writer, String.valueOf(reader.getByte()), fieldName);
    } else if (readNumbersAsDouble) {
      writer.float8(fieldName).writeFloat8(reader.getByte());
    } else {
      ((writer.map != null) ? writer.map.tinyInt(fieldName) : writer.list.tinyInt()).writeTinyInt(reader.getByte());
    }
  }

  private void writeBoolean(MapOrListWriterImpl writer, String fieldName, DBDocumentReaderBase reader) {
    if (allTextMode) {
      writeString(writer, String.valueOf(reader.getBoolean()), fieldName);
    } else {
      writer.bit(fieldName).writeBit(reader.getBoolean() ? 1 : 0);
    }
  }

  private void writeBinary(MapOrListWriterImpl writer, String fieldName, ByteBuffer buf) {
    if (allTextMode) {
      writeString(writer, fieldName, Bytes.toString(buf));
    } else {
      buffer = buffer.reallocIfNeeded(buf.remaining());
      buffer.setBytes(0, buf, buf.position(), buf.remaining());
      writer.binary(fieldName).writeVarBinary(0, buf.remaining(), buffer);
    }
  }

  private void writeString(MapOrListWriterImpl writer, String fieldName, String value) {
    final byte[] strBytes = Bytes.toBytes(value);
    buffer = buffer.reallocIfNeeded(strBytes.length);
    buffer.setBytes(0, strBytes);
    writer.varChar(fieldName).writeVarChar(0, strBytes.length, buffer);
  }

  private UserException unsupportedError(String format, Object... args) {
    return UserException.unsupportedError()
        .message(String.format(format, args))
        .build(logger);
  }

  private UserException dataReadError(Throwable t) {
    return dataReadError(t, null);
  }

  private UserException dataReadError(String format, Object... args) {
    return dataReadError(null, format, args);
  }

  private UserException dataReadError(Throwable t, String format, Object... args) {
    return UserException.dataReadError(t)
        .message(format == null ? null : String.format(format, args))
        .build(logger);
  }

  private SchemaChangeException schemaChangeException(Throwable t, String format, Object... args) {
    return new SchemaChangeException(format, t, args);
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
      throw dataReadError(e);
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
