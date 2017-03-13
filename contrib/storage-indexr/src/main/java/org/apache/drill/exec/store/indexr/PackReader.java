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

import org.apache.drill.exec.vector.BigIntVector;
import org.apache.drill.exec.vector.DateVector;
import org.apache.drill.exec.vector.Float4Vector;
import org.apache.drill.exec.vector.Float8Vector;
import org.apache.drill.exec.vector.IntVector;
import org.apache.drill.exec.vector.TimeStampVector;
import org.apache.drill.exec.vector.TimeVector;
import org.apache.drill.exec.vector.VarCharVector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

import io.indexr.data.BytePiece;
import io.indexr.data.BytePieceSetter;
import io.indexr.data.DoubleSetter;
import io.indexr.data.FloatSetter;
import io.indexr.data.IntSetter;
import io.indexr.data.LongSetter;
import io.indexr.segment.Column;
import io.indexr.segment.ColumnSchema;
import io.indexr.segment.SQLType;
import io.indexr.segment.Segment;
import io.indexr.segment.pack.DataPack;
import io.indexr.util.MemoryUtil;

class PackReader {
  private static final int MAX_ROW_COUNT_PER_STEP = DataPack.MAX_COUNT >> 1;
  private static final int MIN_ROW_COUNT_PER_STEP = 64;
  private static final int DEFAULT_STEP_VECTOR_SIZE = 248; // L2 cache size, with some room for others.
  private static final int STEP_VECTOR_SIZE;

  static {
    STEP_VECTOR_SIZE = Integer.parseInt(System.getProperty("indexr.vector.size.kb", String.valueOf(DEFAULT_STEP_VECTOR_SIZE))) << 10;
  }

  private static final Logger logger = LoggerFactory.getLogger(PackReader.class);

  private Segment segment;
  private ProjectedColumnInfo[] columnInfos;
  private int[] columnIds;
  private int packId;
  private int packRowCount;
  private int stepRowCount;
  private int rowOffset;

  PackReader() {
    clear();
  }


  private void clear() {
    segment = null;
    packId = -1;
    packRowCount = -1;
    stepRowCount = -1;
    rowOffset = -1;
  }

  boolean hasMore() {
    return segment != null & rowOffset < packRowCount;
  }

  void setPack(Segment segment, int packId, ProjectedColumnInfo[] columnInfos, int[] columnIds) throws IOException {
    assert !hasMore();

    int packRowCount = -1;
    int projectSizePerRow = 0;
    for (int projectId = 0; projectId < columnInfos.length; projectId++) {
      ColumnSchema cs = columnInfos[projectId].columnSchema;
      int columnId = columnIds[projectId];
      Column column = segment.column(columnId);
      DataPack pack = column.pack(packId);

      assert packRowCount == -1 || packRowCount == pack.objCount();

      packRowCount = pack.objCount();
      projectSizePerRow += Math.ceil(((double) pack.size()) / packRowCount);
    }

    this.columnInfos = columnInfos;
    this.columnIds = columnIds;
    this.segment = segment;
    this.packId = packId;
    this.packRowCount = packRowCount;
    this.stepRowCount = Math.min(Math.max(STEP_VECTOR_SIZE / projectSizePerRow, MIN_ROW_COUNT_PER_STEP), MAX_ROW_COUNT_PER_STEP);
    this.rowOffset = 0;

    logger.debug(
        "packId:{}, packRowCount:{}, stepRowCount:{}, rowOffset:{}",
        packId, packRowCount, stepRowCount, rowOffset);

    if (!hasMore()) {
      clear();
    }
  }

  int read() throws IOException {
    Preconditions.checkState(hasMore());

    int offset = rowOffset;
    int count = Math.min(packRowCount - offset, stepRowCount);
    rowOffset = offset + count;

    for (int projectId = 0; projectId < columnInfos.length; projectId++) {
      ProjectedColumnInfo projectInfo = columnInfos[projectId];
      int columnId = columnIds[projectId];
      DataPack dataPack = segment.column(columnId).pack(packId);
      SQLType sqlType = projectInfo.columnSchema.getSqlType();
      switch (sqlType) {
        case INT: {
          IntVector.Mutator mutator = (IntVector.Mutator) projectInfo.valueVector.getMutator();
          // Force the vector to allocate engough space.
          mutator.setSafe(count - 1, 0);
          dataPack.foreach(offset, count, new IntSetter() {
            @Override
            public void set(int id, int value) {
              mutator.set(id - offset, value);
            }
          });
          break;
        }
        case BIGINT: {
          BigIntVector.Mutator mutator = (BigIntVector.Mutator) projectInfo.valueVector.getMutator();
          mutator.setSafe(count - 1, 0);
          dataPack.foreach(offset, count, new LongSetter() {
            @Override
            public void set(int id, long value) {
              mutator.set(id - offset, value);
            }
          });
          break;
        }
        case FLOAT: {
          Float4Vector.Mutator mutator = (Float4Vector.Mutator) projectInfo.valueVector.getMutator();
          mutator.setSafe(count - 1, 0);
          dataPack.foreach(offset, count, new FloatSetter() {
            @Override
            public void set(int id, float value) {
              mutator.set(id - offset, value);
            }
          });
          break;
        }
        case DOUBLE: {
          Float8Vector.Mutator mutator = (Float8Vector.Mutator) projectInfo.valueVector.getMutator();
          mutator.setSafe(count - 1, 0);
          dataPack.foreach(offset, count, new DoubleSetter() {
            @Override
            public void set(int id, double value) {
              mutator.set(id - offset, value);
            }
          });
          break;
        }
        case DATE: {
          DateVector.Mutator mutator = (DateVector.Mutator) projectInfo.valueVector.getMutator();
          mutator.setSafe(count - 1, 0);
          dataPack.foreach(offset, count, new LongSetter() {
            @Override
            public void set(int id, long value) {
              mutator.set(id - offset, value);
            }
          });
          break;
        }
        case TIME: {
          TimeVector.Mutator mutator = (TimeVector.Mutator) projectInfo.valueVector.getMutator();
          mutator.setSafe(count - 1, 0);
          dataPack.foreach(offset, count, new IntSetter() {
            @Override
            public void set(int id, int value) {
              mutator.set(id - offset, value);
            }
          });
          break;
        }
        case DATETIME: {
          TimeStampVector.Mutator mutator = (TimeStampVector.Mutator) projectInfo.valueVector.getMutator();
          mutator.setSafe(count - 1, 0);
          dataPack.foreach(offset, count, new LongSetter() {
            @Override
            public void set(int id, long value) {
              mutator.set(id - offset, value);
            }
          });
          break;
        }
        case VARCHAR: {
          ByteBuffer byteBuffer = MemoryUtil.getHollowDirectByteBuffer();
          VarCharVector.Mutator mutator = (VarCharVector.Mutator) projectInfo.valueVector.getMutator();
          dataPack.foreach(offset, count,
              new BytePieceSetter() {
                @Override
                public void set(int id, BytePiece bytes) {
                  assert bytes.base == null;
                  MemoryUtil.setByteBuffer(byteBuffer, bytes.addr, bytes.len, null);
                  mutator.setSafe(id - offset, byteBuffer, 0, byteBuffer.remaining());
                }
              });
          break;
        }
        default:
          throw new IllegalStateException(String.format("Unsupported date type %s", projectInfo.columnSchema.getSqlType()));
      }
    }

    if (!hasMore()) {
      clear();
    }
    return count;
  }
}
