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

import com.google.common.base.Preconditions;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.vector.BaseDataValueVector;
import org.apache.drill.exec.vector.BigIntVector;
import org.apache.drill.exec.vector.Float4Vector;
import org.apache.drill.exec.vector.Float8Vector;
import org.apache.drill.exec.vector.IntVector;
import org.apache.drill.exec.vector.UInt4Vector;
import org.apache.drill.exec.vector.VarCharVector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import io.indexr.data.BytePiece;
import io.indexr.data.BytePieceSetter;
import io.indexr.data.DoubleSetter;
import io.indexr.data.FloatSetter;
import io.indexr.data.IntSetter;
import io.indexr.data.LongSetter;
import io.indexr.io.ByteSlice;
import io.indexr.segment.ColumnSchema;
import io.indexr.segment.ColumnType;
import io.indexr.segment.RSValue;
import io.indexr.segment.Segment;
import io.indexr.segment.SegmentSchema;
import io.indexr.segment.helper.SegmentOpener;
import io.indexr.segment.helper.SingleWork;
import io.indexr.segment.pack.DataPack;
import io.indexr.segment.pack.Version;
import io.indexr.segment.rc.Attr;
import io.indexr.segment.rc.RCOperator;
import io.indexr.util.MemoryUtil;
import io.netty.buffer.DrillBuf;

public class IndexRRecordReaderByPack extends IndexRRecordReader {
  private static final Logger log = LoggerFactory.getLogger(IndexRRecordReaderByPack.class);

  private RCOperator rsFilter;
  private int curStepId = 0;

  private long getPackTime = 0;
  private long setValueTime = 0;
  private long lmCheckTime = 0;

  private boolean isLateMaterialization = false;
  private Segment curSegment;
  private int[] projectColumnIds;

  public IndexRRecordReaderByPack(String tableName,//
                                  SegmentSchema schema,//
                                  List<SchemaPath> projectColumns,//
                                  SegmentOpener segmentOpener,//
                                  RCOperator rsFilter,//
                                  List<SingleWork> works) {
    super(tableName, schema, segmentOpener, projectColumns, works);
    this.segmentOpener = segmentOpener;
    this.rsFilter = rsFilter;
    this.works = works;
  }

  @Override
  public void setup(OperatorContext context, OutputMutator output) throws ExecutionSetupException {
    super.setup(context, output);

    projectColumnIds = new int[projectColumnInfos.length];

    if (rsFilter != null) {
      Set<String> predicateColumns = new HashSet<>();
      rsFilter.foreach(
          new Consumer<RCOperator>() {
            @Override
            public void accept(RCOperator op) {
              for (Attr attr : op.attr()) {
                predicateColumns.add(attr.columnName());
              }
            }
          });
      // The late materialization is worthy only when there are columns not included in predicates.
      isLateMaterialization = predicateColumns.size() < projectColumnInfos.length;
    }
  }

  @Override
  public int next() {
    int read = -1;
    while (read <= 0) {
      if (curStepId >= works.size()) {
        return 0;
      }

      SingleWork stepWork = works.get(curStepId);
      curStepId++;

      Segment segment = segmentMap.get(stepWork.segment());
      int packId = stepWork.packId();
      try {
        read = read(segment, packId);
      } catch (Throwable t) {
        // No matter or what, don't thrown exception from here.
        // It will break the Drill algorithm and make system unpredictable.
        // I do think Drill should handle this...

        log.error("Read rows error, query may return incorrect result.", t);
        read = 0;
      }
    }
    return read;
  }

  /**
   * This method check whether those rows in packId possibly contains any rows we interested.
   *
   * @return null means we can ignore those rows of packId.
   * Otherwise an pack array which some packs may have already been loaded into.
   */
  private DataPack[] beforeRead(Segment segment, int packId) throws IOException {
    List<ColumnSchema> schemas = segment.schema().getColumns();
    DataPack[] rowPacks = new DataPack[schemas.size()];
    if (curSegment != segment) {
      curSegment = segment;

      // Set the attrs to the real columnIds.
      if (rsFilter != null) {
        rsFilter.materialize(schemas);
      }
      // Set the project columns to the real columnIds.
      for (int i = 0; i < projectColumnInfos.length; i++) {
        ColumnSchema column = projectColumnInfos[i].columnSchema;
        Integer columnId = DrillIndexRTable.mapColumn(column, segment.schema());
        if (columnId == null) {
          throw new IllegalStateException(String.format("segment[%s]: column %s not found in %s",
              segment.name(), column, segment.schema()));
        }
        projectColumnIds[i] = columnId;
      }
    }

    if (!isLateMaterialization || rsFilter == null) {
      return rowPacks;
    }

    long time = System.currentTimeMillis();

    rsFilter.foreachEX(
        new RCOperator.OpConsumer() {
          @Override
          public void accept(RCOperator op) throws IOException {
            for (Attr attr : op.attr()) {
              int columnId = attr.columnId();
              if (rowPacks[columnId] == null) {
                rowPacks[columnId] = (DataPack) segment.column(columnId).pack(packId);
              }
            }
          }
        });

    long time2 = System.currentTimeMillis();
    getPackTime = time2 - time;

    byte res = rsFilter.roughCheckOnRow(rowPacks);
    lmCheckTime += System.currentTimeMillis() - time2;

    return res == RSValue.None ? null : rowPacks;
  }

  private int read(Segment segment, int packId) throws IOException {
    DataPack[] rowPacks = beforeRead(segment, packId);
    if (rowPacks == null) {
      log.debug("rsFilter ignore (LM) segment {}, pack: {}", segment.name(), packId);
      return 0;
    }

    int read = -1;
    for (int projectId = 0; projectId < projectColumnInfos.length; projectId++) {
      ProjectedColumnInfo projectInfo = projectColumnInfos[projectId];
      int columnId = projectColumnIds[projectId];

      long time = System.currentTimeMillis();

      DataPack dataPack = rowPacks[columnId];
      if (dataPack == null) {
        dataPack = (DataPack) segment.column(columnId).pack(packId);
        rowPacks[columnId] = dataPack;
      }

      long time2 = System.currentTimeMillis();
      getPackTime += time2 - time;

      byte dataType = projectInfo.columnSchema.dataType;
      int count = dataPack.count();
      if (count == 0) {
        log.warn("segment[{}]: found empty pack, packId: [{}]", segment.name(), packId);
        return 0;
      }
      if (read == -1) {
        read = count;
      }
      assert read == count;

      if (dataPack.version() == Version.VERSION_0_ID) {
        switch (dataType) {
          case ColumnType.INT: {
            IntVector.Mutator mutator = (IntVector.Mutator) projectInfo.valueVector.getMutator();
            // Force the vector to allocate engough space.
            mutator.setSafe(count - 1, 0);
            dataPack.foreach(0, count, new IntSetter() {
              @Override
              public void set(int id, int value) {
                mutator.set(id, value);
              }
            });
            break;
          }
          case ColumnType.LONG: {
            BigIntVector.Mutator mutator = (BigIntVector.Mutator) projectInfo.valueVector.getMutator();
            mutator.setSafe(count - 1, 0);
            dataPack.foreach(0, count, new LongSetter() {
              @Override
              public void set(int id, long value) {
                mutator.set(id, value);
              }
            });
            break;
          }
          case ColumnType.FLOAT: {
            Float4Vector.Mutator mutator = (Float4Vector.Mutator) projectInfo.valueVector.getMutator();
            mutator.setSafe(count - 1, 0);
            dataPack.foreach(0, count, new FloatSetter() {
              @Override
              public void set(int id, float value) {
                mutator.set(id, value);
              }
            });
            break;
          }
          case ColumnType.DOUBLE: {
            Float8Vector.Mutator mutator = (Float8Vector.Mutator) projectInfo.valueVector.getMutator();
            mutator.setSafe(count - 1, 0);
            dataPack.foreach(0, count, new DoubleSetter() {
              @Override
              public void set(int id, double value) {
                mutator.set(id, value);
              }
            });
            break;
          }
          case ColumnType.STRING: {
            ByteBuffer byteBuffer = MemoryUtil.getHollowDirectByteBuffer();
            VarCharVector.Mutator mutator = (VarCharVector.Mutator) projectInfo.valueVector.getMutator();
            dataPack.foreach(0, count,
                new BytePieceSetter() {
                  @Override
                  public void set(int id, BytePiece bytes) {
                    assert bytes.base == null;
                    MemoryUtil.setByteBuffer(byteBuffer, bytes.addr, bytes.len, null);
                    mutator.setSafe(id, byteBuffer, 0, byteBuffer.remaining());
                  }
                });
            break;
          }
          default:
            throw new IllegalStateException(String.format("Unsupported date type %s", projectInfo.columnSchema.dataType));
        }
      } else {
        // Start from v1, we directly copy the memory into vector, to avoid the traversing cost.

        if (dataType == ColumnType.STRING) {
          VarCharVector vector = (VarCharVector) projectInfo.valueVector;
          UInt4Vector offsetVector = vector.getOffsetVector();

          ByteSlice packData = dataPack.data();
          int indexSize = (count + 1) << 2;
          int strDataSize = packData.size() - indexSize;

          // Expand the offset vector if needed.
          offsetVector.getMutator().setSafe(count, 0);
          // Expand the data vector if needed.
          while (vector.getByteCapacity() < strDataSize) {
            vector.reAlloc();
          }
          Preconditions.checkState(vector.getByteCapacity() >= strDataSize, "Illegal drill vector buff capacity");

          DrillBuf offsetBuffer = offsetVector.getBuffer();
          DrillBuf vectorBuffer = vector.getBuffer();

          MemoryUtil.copyMemory(packData.address(), offsetBuffer.memoryAddress(), indexSize);
          MemoryUtil.copyMemory(packData.address() + indexSize, vectorBuffer.memoryAddress(), strDataSize);
        } else {
          BaseDataValueVector vector = (BaseDataValueVector) projectInfo.valueVector;

          // Expand the vector if needed.
          switch (dataType) {
            case ColumnType.INT:
              ((IntVector.Mutator) vector.getMutator()).setSafe(count - 1, 0);
              break;
            case ColumnType.LONG:
              ((BigIntVector.Mutator) vector.getMutator()).setSafe(count - 1, 0);
              break;
            case ColumnType.FLOAT:
              ((Float4Vector.Mutator) vector.getMutator()).setSafe(count - 1, 0);
              break;
            case ColumnType.DOUBLE:
              ((Float8Vector.Mutator) vector.getMutator()).setSafe(count - 1, 0);
              break;
            default:
              throw new IllegalStateException(String.format("Unsupported date type %s", projectInfo.columnSchema.dataType));
          }

          DrillBuf vectorBuffer = vector.getBuffer();
          ByteSlice packData = dataPack.data();

          Preconditions.checkState((count << ColumnType.numTypeShift(dataType)) == packData.size(), "Illegal pack size");
          Preconditions.checkState(vectorBuffer.capacity() >= packData.size(), "Illegal drill vector buff capacity");

          MemoryUtil.copyMemory(packData.address(), vectorBuffer.memoryAddress(), packData.size());
        }
      }
      setValueTime += System.currentTimeMillis() - time2;
    }
    return read;
  }

  @Override
  public void close() throws Exception {
    super.close();
    log.debug("cost: getPack: {}ms, setValue: {}ms, lmCheck: {}ms", getPackTime, setValueTime, lmCheckTime);
  }
}
