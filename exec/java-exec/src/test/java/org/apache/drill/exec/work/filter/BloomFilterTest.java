/*
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
package org.apache.drill.exec.work.filter;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.scanner.ClassPathScanner;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.expr.ValueVectorReadExpression;
import org.apache.drill.exec.expr.fn.FunctionImplementationRegistry;
import org.apache.drill.exec.expr.fn.impl.ValueVectorHashHelper;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.FragmentContextImpl;
import org.apache.drill.exec.proto.BitControl;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.TypedFieldId;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.record.WritableBatch;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.record.selection.SelectionVector4;
import org.apache.drill.exec.server.Drillbit;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.server.RemoteServiceSet;
import org.apache.drill.exec.vector.VarCharVector;
import org.junit.Assert;
import org.junit.Test;
import java.util.Iterator;

public class BloomFilterTest {
  public static DrillConfig c = DrillConfig.create();

  class TestRecordBatch implements RecordBatch {
    private final VectorContainer container;

    public TestRecordBatch(VectorContainer container) {
      this.container = container;

    }

    @Override
    public int getRecordCount() {
      return 0;
    }

    @Override
    public SelectionVector2 getSelectionVector2() {
      return null;
    }

    @Override
    public SelectionVector4 getSelectionVector4() {
      return null;
    }

    @Override
    public FragmentContext getContext() {
      return null;
    }

    @Override
    public BatchSchema getSchema() {
      return null;
    }

    @Override
    public void kill(boolean sendUpstream) {

    }

    @Override
    public VectorContainer getOutgoingContainer() {
      return null;
    }

    @Override
    public VectorContainer getContainer() {
      return null;
    }

    @Override
    public TypedFieldId getValueVectorId(SchemaPath path) {
      return container.getValueVectorId(path);
    }

    @Override
    public VectorWrapper<?> getValueAccessorById(Class<?> clazz, int... ids) {
      return container.getValueAccessorById(clazz, ids);
    }

    @Override
    public IterOutcome next() {
      return null;
    }

    @Override
    public WritableBatch getWritableBatch() {
      return null;
    }

    @Override
    public Iterator<VectorWrapper<?>> iterator() {
      return null;
    }

    @Override
    public void dump() {
    }

    @Override
    public boolean hasFailed() {
      return false;
    }
  }


  @Test
  public void testNotExist() throws Exception {
    Drillbit bit = new Drillbit(c, RemoteServiceSet.getLocalServiceSet(), ClassPathScanner.fromPrescan(c));
    bit.run();
    DrillbitContext bitContext = bit.getContext();
    FunctionImplementationRegistry registry = bitContext.getFunctionImplementationRegistry();
    FragmentContextImpl context = new FragmentContextImpl(bitContext, BitControl.PlanFragment.getDefaultInstance(), null, registry);
    BufferAllocator bufferAllocator = bitContext.getAllocator();
    //create RecordBatch
    VarCharVector vector = new VarCharVector(SchemaBuilder.columnSchema("a", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.REQUIRED), bufferAllocator);
    vector.allocateNew();
    int valueCount = 3;
    VarCharVector.Mutator mutator = vector.getMutator();
    mutator.setSafe(0, "a".getBytes());
    mutator.setSafe(1, "b".getBytes());
    mutator.setSafe(2, "c".getBytes());
    mutator.setValueCount(valueCount);
    VectorContainer vectorContainer = new VectorContainer();
    TypedFieldId fieldId = vectorContainer.add(vector);
    RecordBatch recordBatch = new TestRecordBatch(vectorContainer);
    //construct hash64
    ValueVectorReadExpression exp = new ValueVectorReadExpression(fieldId);
    LogicalExpression[] expressions = new LogicalExpression[1];
    expressions[0] = exp;
    TypedFieldId[] fieldIds = new TypedFieldId[1];
    fieldIds[0] = fieldId;
    ValueVectorHashHelper valueVectorHashHelper = new ValueVectorHashHelper(recordBatch, context);
    ValueVectorHashHelper.Hash64 hash64 = valueVectorHashHelper.getHash64(expressions, fieldIds);

    //construct BloomFilter
    int numBytes = BloomFilter.optimalNumOfBytes(3, 0.03);

    BloomFilter bloomFilter = new BloomFilter(numBytes, bufferAllocator);
    for (int i = 0; i < valueCount; i++) {
      long hashCode = hash64.hash64Code(i, 0, 0);
      bloomFilter.insert(hashCode);
    }

    //-----------------create probe side RecordBatch---------------------
    VarCharVector probeVector = new VarCharVector(SchemaBuilder.columnSchema("a", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.REQUIRED), bufferAllocator);
    probeVector.allocateNew();
    int probeValueCount = 1;
    VarCharVector.Mutator mutator1 = probeVector.getMutator();
    mutator1.setSafe(0, "f".getBytes());
    mutator1.setValueCount(probeValueCount);
    VectorContainer probeVectorContainer = new VectorContainer();
    TypedFieldId probeFieldId = probeVectorContainer.add(probeVector);
    RecordBatch probeRecordBatch = new TestRecordBatch(probeVectorContainer);
    ValueVectorReadExpression probExp = new ValueVectorReadExpression(probeFieldId);
    LogicalExpression[] probExpressions = new LogicalExpression[1];
    probExpressions[0] = probExp;
    TypedFieldId[] probeFieldIds = new TypedFieldId[1];
    probeFieldIds[0] = probeFieldId;
    ValueVectorHashHelper probeValueVectorHashHelper = new ValueVectorHashHelper(probeRecordBatch, context);
    ValueVectorHashHelper.Hash64 probeHash64 = probeValueVectorHashHelper.getHash64(probExpressions, probeFieldIds);
    long hashCode = probeHash64.hash64Code(0, 0, 0);
    boolean contain = bloomFilter.find(hashCode);
    Assert.assertFalse(contain);
    bloomFilter.getContent().close();
    vectorContainer.clear();
    probeVectorContainer.clear();
    context.close();
    bitContext.close();
    bit.close();
  }


  @Test
  public void testExist() throws Exception {

    Drillbit bit = new Drillbit(c, RemoteServiceSet.getLocalServiceSet(), ClassPathScanner.fromPrescan(c));
    bit.run();
    DrillbitContext bitContext = bit.getContext();
    FunctionImplementationRegistry registry = bitContext.getFunctionImplementationRegistry();
    FragmentContextImpl context = new FragmentContextImpl(bitContext, BitControl.PlanFragment.getDefaultInstance(), null, registry);
    BufferAllocator bufferAllocator = bitContext.getAllocator();
    //create RecordBatch
    VarCharVector vector = new VarCharVector(SchemaBuilder.columnSchema("a", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.REQUIRED), bufferAllocator);
    vector.allocateNew();
    int valueCount = 3;
    VarCharVector.Mutator mutator = vector.getMutator();
    mutator.setSafe(0, "a".getBytes());
    mutator.setSafe(1, "b".getBytes());
    mutator.setSafe(2, "c".getBytes());
    mutator.setValueCount(valueCount);
    VectorContainer vectorContainer = new VectorContainer();
    TypedFieldId fieldId = vectorContainer.add(vector);
    RecordBatch recordBatch = new TestRecordBatch(vectorContainer);
    //construct hash64
    ValueVectorReadExpression exp = new ValueVectorReadExpression(fieldId);
    LogicalExpression[] expressions = new LogicalExpression[1];
    expressions[0] = exp;
    TypedFieldId[] fieldIds = new TypedFieldId[1];
    fieldIds[0] = fieldId;
    ValueVectorHashHelper valueVectorHashHelper = new ValueVectorHashHelper(recordBatch, context);
    ValueVectorHashHelper.Hash64 hash64 = valueVectorHashHelper.getHash64(expressions, fieldIds);

    //construct BloomFilter
    int numBytes = BloomFilter.optimalNumOfBytes(3, 0.03);

    BloomFilter bloomFilter = new BloomFilter(numBytes, bufferAllocator);
    for (int i = 0; i < valueCount; i++) {
      long hashCode = hash64.hash64Code(i, 0, 0);
      bloomFilter.insert(hashCode);
    }

    //-----------------create probe side RecordBatch---------------------
    VarCharVector probeVector = new VarCharVector(SchemaBuilder.columnSchema("a", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.REQUIRED), bufferAllocator);
    probeVector.allocateNew();
    int probeValueCount = 1;
    VarCharVector.Mutator mutator1 = probeVector.getMutator();
    mutator1.setSafe(0, "a".getBytes());
    mutator1.setValueCount(probeValueCount);
    VectorContainer probeVectorContainer = new VectorContainer();
    TypedFieldId probeFieldId = probeVectorContainer.add(probeVector);
    RecordBatch probeRecordBatch = new TestRecordBatch(probeVectorContainer);
    ValueVectorReadExpression probExp = new ValueVectorReadExpression(probeFieldId);
    LogicalExpression[] probExpressions = new LogicalExpression[1];
    probExpressions[0] = probExp;
    TypedFieldId[] probeFieldIds = new TypedFieldId[1];
    probeFieldIds[0] = probeFieldId;
    ValueVectorHashHelper probeValueVectorHashHelper = new ValueVectorHashHelper(probeRecordBatch, context);
    ValueVectorHashHelper.Hash64 probeHash64 = probeValueVectorHashHelper.getHash64(probExpressions, probeFieldIds);
    long hashCode = probeHash64.hash64Code(0, 0, 0);
    boolean contain = bloomFilter.find(hashCode);
    Assert.assertTrue(contain);
    bloomFilter.getContent().close();
    vectorContainer.clear();
    probeVectorContainer.clear();
    context.close();
    bitContext.close();
    bit.close();
  }


  @Test
  public void testMerged() throws Exception {

    Drillbit bit = new Drillbit(c, RemoteServiceSet.getLocalServiceSet(), ClassPathScanner.fromPrescan(c));
    bit.run();
    DrillbitContext bitContext = bit.getContext();
    FunctionImplementationRegistry registry = bitContext.getFunctionImplementationRegistry();
    FragmentContextImpl context = new FragmentContextImpl(bitContext, BitControl.PlanFragment.getDefaultInstance(), null, registry);
    BufferAllocator bufferAllocator = bitContext.getAllocator();
    //create RecordBatch
    VarCharVector vector = new VarCharVector(SchemaBuilder.columnSchema("a", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.REQUIRED), bufferAllocator);
    vector.allocateNew();
    int valueCount = 3;
    VarCharVector.Mutator mutator = vector.getMutator();
    mutator.setSafe(0, "a".getBytes());
    mutator.setSafe(1, "b".getBytes());
    mutator.setSafe(2, "c".getBytes());
    mutator.setValueCount(valueCount);
    VectorContainer vectorContainer = new VectorContainer();
    TypedFieldId fieldId = vectorContainer.add(vector);
    RecordBatch recordBatch = new TestRecordBatch(vectorContainer);
    //construct hash64
    ValueVectorReadExpression exp = new ValueVectorReadExpression(fieldId);
    LogicalExpression[] expressions = new LogicalExpression[1];
    expressions[0] = exp;
    TypedFieldId[] fieldIds = new TypedFieldId[1];
    fieldIds[0] = fieldId;
    ValueVectorHashHelper valueVectorHashHelper = new ValueVectorHashHelper(recordBatch, context);
    ValueVectorHashHelper.Hash64 hash64 = valueVectorHashHelper.getHash64(expressions, fieldIds);

    //construct BloomFilter
    int numBytes = BloomFilter.optimalNumOfBytes(3, 0.03);

    BloomFilter bloomFilter = new BloomFilter(numBytes, bufferAllocator);
    for (int i = 0; i < valueCount; i++) {
      long hashCode = hash64.hash64Code(i, 0, 0);
      bloomFilter.insert(hashCode);
    }

    BloomFilter bloomFilter1 = new BloomFilter(numBytes, bufferAllocator);
    for (int i = 0; i < valueCount; i++) {
      long hashCode = hash64.hash64Code(i, 0, 0);
      bloomFilter1.insert(hashCode);
    }

    bloomFilter.or(bloomFilter1);

    //-----------------create probe side RecordBatch---------------------
    VarCharVector probeVector = new VarCharVector(SchemaBuilder.columnSchema("a", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.REQUIRED), bufferAllocator);
    probeVector.allocateNew();
    int probeValueCount = 1;
    VarCharVector.Mutator mutator1 = probeVector.getMutator();
    mutator1.setSafe(0, "a".getBytes());
    mutator1.setValueCount(probeValueCount);
    VectorContainer probeVectorContainer = new VectorContainer();
    TypedFieldId probeFieldId = probeVectorContainer.add(probeVector);
    RecordBatch probeRecordBatch = new TestRecordBatch(probeVectorContainer);
    ValueVectorReadExpression probExp = new ValueVectorReadExpression(probeFieldId);
    LogicalExpression[] probExpressions = new LogicalExpression[1];
    probExpressions[0] = probExp;
    TypedFieldId[] probeFieldIds = new TypedFieldId[1];
    probeFieldIds[0] = probeFieldId;
    ValueVectorHashHelper probeValueVectorHashHelper = new ValueVectorHashHelper(probeRecordBatch, context);
    ValueVectorHashHelper.Hash64 probeHash64 = probeValueVectorHashHelper.getHash64(probExpressions, probeFieldIds);
    long hashCode = probeHash64.hash64Code(0, 0, 0);
    boolean contain = bloomFilter.find(hashCode);
    Assert.assertTrue(contain);
    bloomFilter.getContent().close();
    vectorContainer.clear();
    probeVectorContainer.clear();
    context.close();
    bitContext.close();
    bit.close();
  }
}
