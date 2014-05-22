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
import java.util.Map;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.ExecTest;
import org.apache.drill.exec.cache.ProtoSerializable.FragmentHandleSerializable;
import org.apache.drill.exec.cache.infinispan.ICache;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.memory.TopLevelAllocator;
import org.apache.drill.exec.planner.logical.StoragePlugins;
import org.apache.drill.exec.proto.BitControl.FragmentStatus;
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
import org.apache.drill.exec.store.dfs.FileSystemConfig;
import org.apache.drill.exec.vector.AllocationHelper;
import org.apache.drill.exec.vector.IntVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.VarBinaryVector;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.hive12.common.collect.Maps;

public class TestCacheSerialization extends ExecTest {

  private static DistributedCache ICACHE;
  private static BufferAllocator ALLOCATOR;
  private static final DrillConfig CONFIG = DrillConfig.create();

  @Test
  public void testProtobufSerialization() {
    DistributedMap<FragmentHandleSerializable> map = ICACHE.getMap(FragmentHandleSerializable.class);
    FragmentHandle h = FragmentHandle.newBuilder().setMajorFragmentId(1).setMinorFragmentId(1).setQueryId(QueryId.newBuilder().setPart1(74).setPart2(66).build()).build();
    FragmentHandleSerializable s = new FragmentHandleSerializable(h);
    map.put("1", s);
    for(int i =0; i < 2; i++){
      FragmentHandleSerializable s2 = map.get("1");
      Assert.assertEquals(s.getObject(), s2.getObject());
    }
  }

//  @Test
//  public void testProtobufExternalizer(){
//    final FragmentStatus fs = FragmentStatus.newBuilder().setHandle(FragmentHandle.newBuilder().setMajorFragmentId(1).setMajorFragmentId(35)).build();
//    DistributedMap<OptionValue> map = ICACHE.getNamedMap(FragmentStatus.class);
//    map.put("1", v);
//    for(int i = 0; i < 5; i++){
//      OptionValue v2 = map.get("1");
//      Assert.assertEquals(v, v2);
//    }
//  }

  @Test
  public void testJackSerializable(){
    OptionValue v = OptionValue.createBoolean(OptionType.SESSION, "my test option", true);
    DistributedMap<OptionValue> map = ICACHE.getNamedMap("sys.options", OptionValue.class);
    map.put("1", v);
    for(int i = 0; i < 5; i++){
      OptionValue v2 = map.get("1");
      Assert.assertEquals(v, v2);
    }
  }

  @Test
  public void testCustomJsonSerialization(){
    Map<String, StoragePluginConfig> configs = Maps.newHashMap();
    configs.put("hello", new FileSystemConfig());
    StoragePlugins p = new StoragePlugins(configs);

    DistributedMap<StoragePlugins> map = ICACHE.getMap(StoragePlugins.class);
    map.put("1", p);
    for(int i =0; i < 2; i++){
      StoragePlugins p2 = map.get("1");
      Assert.assertEquals(p, p2);
    }
  }

  @Test
  public void testVectorCache() throws Exception {
    List<ValueVector> vectorList = Lists.newArrayList();
    RemoteServiceSet serviceSet = RemoteServiceSet.getLocalServiceSet();

    MaterializedField intField = MaterializedField.create(new SchemaPath("int", ExpressionPosition.UNKNOWN),
        Types.required(TypeProtos.MinorType.INT));
    IntVector intVector = (IntVector) TypeHelper.getNewVector(intField, ALLOCATOR);
    MaterializedField binField = MaterializedField.create(new SchemaPath("binary", ExpressionPosition.UNKNOWN),
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

    DistributedMultiMap<CachedVectorContainer> mmap = ICACHE.getMultiMap(CachedVectorContainer.class);
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

  // @Test
  // public void testHazelVectorCache() throws Exception {
  // DrillConfig c = DrillConfig.create();
  // HazelCache cache = new HazelCache(c, new TopLevelAllocator());
  // cache.run();
  // testCache(c, cache);
  // cache.close();
  // }

  @BeforeClass
  public static void setupCache() throws Exception {
    ALLOCATOR = new TopLevelAllocator();
    ICACHE = new ICache(CONFIG, ALLOCATOR);
    ICACHE.run();
  }

  @AfterClass
  public static void destroyCache() throws Exception {
    ICACHE.close();
    ALLOCATOR.close();
  }

}
