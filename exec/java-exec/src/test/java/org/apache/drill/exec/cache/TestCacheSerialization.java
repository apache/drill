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

import java.util.List;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.ExecTest;
import org.apache.drill.exec.cache.DistributedCache.CacheConfig;
import org.apache.drill.exec.cache.infinispan.ICache;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.memory.TopLevelAllocator;
import org.apache.drill.exec.physical.impl.orderedpartitioner.OrderedPartitionRecordBatch;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.VectorAccessible;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.record.WritableBatch;
import org.apache.drill.exec.server.RemoteServiceSet;
import org.apache.drill.exec.server.options.OptionValue;
import org.apache.drill.exec.server.options.OptionValue.OptionType;
import org.apache.drill.exec.vector.AllocationHelper;
import org.apache.drill.exec.vector.IntVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.VarBinaryVector;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class TestCacheSerialization extends ExecTest {

  private static DistributedCache ICACHE;
  private static BufferAllocator ALLOCATOR;
  private static final DrillConfig CONFIG = DrillConfig.create();

  @Test
  public void protobufSerialization() {
    DistributedMap<String, FragmentHandle> map = ICACHE.getMap(CacheConfig.newBuilder(FragmentHandle.class).proto().build());
    FragmentHandle s = FragmentHandle.newBuilder().setMajorFragmentId(1).setMinorFragmentId(1).setQueryId(QueryId.newBuilder().setPart1(74).setPart2(66).build()).build();
    map.put("1", s);
    for(int i =0; i < 2; i++){
      FragmentHandle s2 = map.get("1");
      Assert.assertEquals(s, s2);
    }
  }

  @Test
  public void jacksonSerialization(){
    OptionValue v = OptionValue.createBoolean(OptionType.SESSION, "my test option", true);
    DistributedMap<String, OptionValue> map = ICACHE.getMap(CacheConfig.newBuilder(OptionValue.class).jackson().build());
    map.put("1", v);
    for(int i = 0; i < 5; i++){
      OptionValue v2 = map.get("1");
      Assert.assertEquals(v, v2);
    }
  }

  @Test
  public void multimapWithDrillSerializable() throws Exception {
    List<ValueVector> vectorList = Lists.newArrayList();

    MaterializedField intField = MaterializedField.create(SchemaPath.getSimplePath("int"),
        Types.required(TypeProtos.MinorType.INT));
    IntVector intVector = (IntVector) TypeHelper.getNewVector(intField, ALLOCATOR);
    MaterializedField binField = MaterializedField.create(SchemaPath.getSimplePath("binary"),
        Types.required(TypeProtos.MinorType.VARBINARY));
    VarBinaryVector binVector = (VarBinaryVector) TypeHelper.getNewVector(binField, ALLOCATOR);
    AllocationHelper.allocate(intVector, 4, 4);
    AllocationHelper.allocate(binVector, 4, 5);
    vectorList.add(intVector);
    vectorList.add(binVector);

    intVector.getMutator().setSafe(0, 0);
    binVector.getMutator().setSafe(0, "ZERO".getBytes());
    intVector.getMutator().setSafe(1, 1);
    binVector.getMutator().setSafe(1, "ONE".getBytes());
    intVector.getMutator().setSafe(2, 2);
    binVector.getMutator().setSafe(2, "TWO".getBytes());
    intVector.getMutator().setSafe(3, 3);
    binVector.getMutator().setSafe(3, "THREE".getBytes());
    intVector.getMutator().setValueCount(4);
    binVector.getMutator().setValueCount(4);

    VectorContainer container = new VectorContainer();
    container.addCollection(vectorList);
    container.setRecordCount(4);
    WritableBatch batch = WritableBatch.getBatchNoHVWrap(container.getRecordCount(), container, false);
    CachedVectorContainer wrap = new CachedVectorContainer(batch, ALLOCATOR);

    DistributedMultiMap<String, CachedVectorContainer> mmap = ICACHE.getMultiMap(OrderedPartitionRecordBatch.MULTI_CACHE_CONFIG);
    mmap.put("vectors", wrap);

    for(int x =0; x < 2; x++){
      CachedVectorContainer newWrap = (CachedVectorContainer) mmap.get("vectors").iterator().next();

      VectorAccessible newContainer = newWrap.get();
      for (VectorWrapper<?> w : newContainer) {
        ValueVector vv = w.getValueVector();
        int values = vv.getAccessor().getValueCount();
        for (int i = 0; i < values; i++) {
          Object o = vv.getAccessor().getObject(i);
          if (o instanceof byte[]) {
            System.out.println(new String((byte[]) o));
          } else {
            System.out.println(o);
          }
        }
      }

      newWrap.clear();
    }
  }

  @BeforeClass
  public static void setupCache() throws Exception {
    ALLOCATOR = new TopLevelAllocator();
    ICACHE = new ICache(CONFIG, ALLOCATOR, true);
    ICACHE.run();
  }

  @AfterClass
  public static void destroyCache() throws Exception {
    ICACHE.close();
    ALLOCATOR.close();
  }

}
