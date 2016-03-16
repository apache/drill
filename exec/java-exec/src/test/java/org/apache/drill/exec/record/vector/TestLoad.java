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
package org.apache.drill.exec.record.vector;

import static org.junit.Assert.assertEquals;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.DrillBuf;

import java.util.List;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.ExecTest;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.memory.RootAllocatorFactory;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.record.WritableBatch;
import org.apache.drill.exec.vector.AllocationHelper;
import org.apache.drill.exec.vector.IntVector;
import org.apache.drill.exec.vector.NullableVarCharVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.VarCharVector;
import org.junit.Test;

import com.google.common.collect.Lists;

public class TestLoad extends ExecTest {
  private final DrillConfig drillConfig = DrillConfig.create();

  @Test
  public void testLoadValueVector() throws Exception {
    final BufferAllocator allocator = RootAllocatorFactory.newRoot(drillConfig);
    final ValueVector fixedV = new IntVector(MaterializedField.create("ints",
        Types.required(MinorType.INT)), allocator);
    final ValueVector varlenV = new VarCharVector(MaterializedField.create(
        "chars", Types.required(MinorType.VARCHAR)), allocator);
    final ValueVector nullableVarlenV = new NullableVarCharVector(MaterializedField.create("chars",
        Types.optional(MinorType.VARCHAR)), allocator);

    final List<ValueVector> vectors = Lists.newArrayList(fixedV, varlenV, nullableVarlenV);
    for (final ValueVector v : vectors) {
      AllocationHelper.allocate(v, 100, 50);
      v.getMutator().generateTestData(100);
    }

    final WritableBatch writableBatch = WritableBatch.getBatchNoHV(100, vectors, false);
    final RecordBatchLoader batchLoader = new RecordBatchLoader(allocator);
    final ByteBuf[] byteBufs = writableBatch.getBuffers();
    int bytes = 0;
    for (int i = 0; i < byteBufs.length; i++) {
      bytes += byteBufs[i].writerIndex();
    }
    final DrillBuf byteBuf = allocator.buffer(bytes);
    int index = 0;
    for (int i = 0; i < byteBufs.length; i++) {
      byteBufs[i].readBytes(byteBuf, index, byteBufs[i].writerIndex());
      index += byteBufs[i].writerIndex();
    }
    byteBuf.writerIndex(bytes);

    batchLoader.load(writableBatch.getDef(), byteBuf);
    boolean firstColumn = true;
    int recordCount = 0;
    for (final VectorWrapper<?> v : batchLoader) {
      if (firstColumn) {
        firstColumn = false;
      } else {
        System.out.print("\t");
      }
      System.out.print(v.getField().getPath());
      System.out.print("[");
      System.out.print(v.getField().getType().getMinorType());
      System.out.print("]");
    }

    System.out.println();
    for (int r = 0; r < batchLoader.getRecordCount(); r++) {
      boolean first = true;
      recordCount++;
      for (final VectorWrapper<?> v : batchLoader) {
        if (first) {
          first = false;
        } else {
          System.out.print("\t");
        }
        final ValueVector.Accessor accessor = v.getValueVector().getAccessor();
        if (v.getField().getType().getMinorType() == TypeProtos.MinorType.VARCHAR) {
          final Object obj = accessor.getObject(r);
          if (obj != null) {
            System.out.print(accessor.getObject(r));
          } else {
            System.out.print("NULL");
          }
        } else {
          System.out.print(accessor.getObject(r));
        }
      }
      if (!first) {
        System.out.println();
      }
    }
    assertEquals(100, recordCount);
    batchLoader.clear();
    writableBatch.clear();
  }
}
