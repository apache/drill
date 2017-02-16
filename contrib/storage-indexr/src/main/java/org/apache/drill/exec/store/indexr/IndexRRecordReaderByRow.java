/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store.indexr;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.vector.BigIntVector;
import org.apache.drill.exec.vector.DateVector;
import org.apache.drill.exec.vector.Float4Vector;
import org.apache.drill.exec.vector.Float8Vector;
import org.apache.drill.exec.vector.IntVector;
import org.apache.drill.exec.vector.TimeStampVector;
import org.apache.drill.exec.vector.TimeVector;
import org.apache.drill.exec.vector.VarCharVector;
import org.apache.spark.unsafe.Platform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;

import io.indexr.data.BytePiece;
import io.indexr.segment.Row;
import io.indexr.segment.SQLType;
import io.indexr.segment.Segment;
import io.indexr.segment.SegmentSchema;
import io.indexr.segment.helper.SegmentOpener;
import io.indexr.segment.helper.SingleWork;
import io.indexr.segment.pack.DataPack;
import io.indexr.util.MemoryUtil;

public class IndexRRecordReaderByRow extends IndexRRecordReader {
  private static final Logger log = LoggerFactory.getLogger(IndexRRecordReaderByRow.class);

  private int nextStepId = 0;
  private Iterator<Row> curIterator;
  private Segment curSegment;

  private long setValueTime = 0;

  public IndexRRecordReaderByRow(String tableName,//
                                 SegmentSchema segmentSchema,//
                                 List<SchemaPath> projectColumns,//
                                 SegmentOpener segmentOpener,//
                                 List<SingleWork> works) {
    super(tableName, segmentSchema, segmentOpener, projectColumns, works);
    this.segmentOpener = segmentOpener;
    this.works = works;
  }

  @Override
  public int next() {
    int read = -1;
    while (read <= 0) {
      if (curIterator == null || !curIterator.hasNext()) {
        if (nextStepId >= works.size()) {
          return 0;
        }
        SingleWork stepWork = works.get(nextStepId);
        nextStepId++;
        curSegment = segmentMap.get(stepWork.segment());
        curIterator = curSegment.rowTraversal().iterator();
      }

      try {
        long time = System.currentTimeMillis();
        read = read(curSegment.schema(), curIterator, DataPack.MAX_COUNT);
        setValueTime += System.currentTimeMillis() - time;
      } catch (Throwable t) {
        log.error("Read rows error, query may return incorrect result.", t);
        read = 0;
      }
    }
    for (ProjectedColumnInfo info : projectColumnInfos) {
      info.valueVector.getMutator().setValueCount(read);
    }

    return read;
  }

  private int read(SegmentSchema schema, Iterator<Row> iterator, int maxRow) {
    int colCount = projectColumnInfos.length;
    SQLType[] sqlTypes = new SQLType[colCount];
    int[] columnIds = new int[colCount];
    for (int i = 0; i < colCount; i++) {
      ProjectedColumnInfo info = projectColumnInfos[i];
      Integer columnId = DrillIndexRTable.mapColumn(info.columnSchema, schema);
      if (columnId == null) {
        log.error("column {} not found in {}", info.columnSchema, schema);
        return -1;
      }
      sqlTypes[i] = info.columnSchema.getSqlType();
      columnIds[i] = columnId;
    }
    int rowId = 0;
    BytePiece bp = new BytePiece();
    ByteBuffer byteBuffer = MemoryUtil.getHollowDirectByteBuffer();
    while (rowId < maxRow && iterator.hasNext()) {
      Row row = iterator.next();
      for (int i = 0; i < colCount; i++) {
        ProjectedColumnInfo info = projectColumnInfos[i];
        int columnId = columnIds[i];
        SQLType sqlType = sqlTypes[i];
        switch (sqlType) {
          case INT: {
            IntVector.Mutator mutator = (IntVector.Mutator) info.valueVector.getMutator();
            mutator.setSafe(rowId, row.getInt(columnId));
            break;
          }
          case BIGINT: {
            BigIntVector.Mutator mutator = (BigIntVector.Mutator) info.valueVector.getMutator();
            mutator.setSafe(rowId, row.getLong(columnId));
            break;
          }
          case FLOAT: {
            Float4Vector.Mutator mutator = (Float4Vector.Mutator) info.valueVector.getMutator();
            mutator.setSafe(rowId, row.getFloat(columnId));
            break;
          }
          case DOUBLE: {
            Float8Vector.Mutator mutator = (Float8Vector.Mutator) info.valueVector.getMutator();
            mutator.setSafe(rowId, row.getDouble(columnId));
            break;
          }
          case DATE: {
            DateVector.Mutator mutator = (DateVector.Mutator) info.valueVector.getMutator();
            mutator.setSafe(rowId, row.getLong(columnId));
            break;
          }
          case TIME: {
            TimeVector.Mutator mutator = (TimeVector.Mutator) info.valueVector.getMutator();
            mutator.setSafe(rowId, row.getInt(columnId));
            break;
          }
          case DATETIME: {
            TimeStampVector.Mutator mutator = (TimeStampVector.Mutator) info.valueVector.getMutator();
            mutator.setSafe(rowId, row.getLong(columnId));
            break;
          }
          case VARCHAR: {
            VarCharVector.Mutator mutator = (VarCharVector.Mutator) info.valueVector.getMutator();
            row.getRaw(columnId, bp);
            if (bp.base != null) {
              assert bp.base instanceof byte[];

              byte[] arr = (byte[]) bp.base;
              mutator.setSafe(rowId, arr, (int) (bp.addr - Platform.BYTE_ARRAY_OFFSET), bp.len);
            } else {
              MemoryUtil.setByteBuffer(byteBuffer, bp.addr, bp.len, null);
              mutator.setSafe(rowId, byteBuffer, 0, byteBuffer.remaining());
            }
            break;
          }
          default:
            throw new IllegalStateException(String.format("Unhandled date type %s", info.columnSchema.getSqlType()));
        }
      }
      rowId++;
    }
    return rowId;
  }

  @Override
  public void close() throws Exception {
    super.close();
    log.debug("cost: setValue: {}ms", setValueTime);
  }
}
