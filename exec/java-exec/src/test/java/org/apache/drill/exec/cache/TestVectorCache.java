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
package org.apache.drill.exec.cache;

import com.beust.jcommander.internal.Lists;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.ExecTest;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.record.*;
import org.apache.drill.exec.server.Drillbit;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.server.RemoteServiceSet;
import org.apache.drill.exec.vector.*;
import org.junit.Test;

import java.util.List;

public class TestVectorCache  extends ExecTest{

  @Test
  public void testVectorCache() throws Exception {
    List<ValueVector> vectorList = Lists.newArrayList();
    RemoteServiceSet serviceSet = RemoteServiceSet.getLocalServiceSet();
    DrillConfig config = DrillConfig.create();
    Drillbit bit = new Drillbit(config, serviceSet);
    bit.run();
    DrillbitContext context = bit.getContext();
    HazelCache cache = new HazelCache(config, context.getAllocator());
    cache.run();

    MaterializedField intField = MaterializedField.create(SchemaPath.getSimplePath("int"), Types.required(TypeProtos.MinorType.INT));
    IntVector intVector = (IntVector)TypeHelper.getNewVector(intField, context.getAllocator());
    MaterializedField binField = MaterializedField.create(new SchemaPath("binary", ExpressionPosition.UNKNOWN), Types.required(TypeProtos.MinorType.VARBINARY));
    VarBinaryVector binVector = (VarBinaryVector)TypeHelper.getNewVector(binField, context.getAllocator());
    AllocationHelper.allocate(intVector, 4, 4);
    AllocationHelper.allocate(binVector, 4, 5);
    vectorList.add(intVector);
    vectorList.add(binVector);

    intVector.getMutator().set(0, 0); binVector.getMutator().set(0, "ZERO".getBytes());
    intVector.getMutator().set(1, 1); binVector.getMutator().set(1, "ONE".getBytes());
    intVector.getMutator().set(2, 2); binVector.getMutator().set(2, "TWO".getBytes());
    intVector.getMutator().set(3, 3); binVector.getMutator().set(3, "THREE".getBytes());
    intVector.getMutator().setValueCount(4);
    binVector.getMutator().setValueCount(4);

    VectorContainer container = new VectorContainer();
    container.addCollection(vectorList);
    container.setRecordCount(4);
    WritableBatch batch = WritableBatch.getBatchNoHVWrap(container.getRecordCount(), container, false);
    VectorAccessibleSerializable wrap = new VectorAccessibleSerializable(batch, context.getAllocator());

    DistributedMultiMap<VectorAccessibleSerializable> mmap = cache.getMultiMap(VectorAccessibleSerializable.class);
    mmap.put("vectors", wrap);
    VectorAccessibleSerializable newWrap = (VectorAccessibleSerializable)mmap.get("vectors").iterator().next();

    VectorAccessible newContainer = newWrap.get();
    for (VectorWrapper w : newContainer) {
      ValueVector vv = w.getValueVector();
      int values = vv.getAccessor().getValueCount();
      for (int i = 0; i < values; i++) {
        Object o = vv.getAccessor().getObject(i);
        if (o instanceof byte[]) {
          System.out.println(new String((byte[])o));
        } else {
          System.out.println(o);
        }
      }
    }
  }
}
