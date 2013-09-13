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
package org.apache.drill.exec.record;

import java.lang.reflect.Array;
import java.util.Iterator;
import java.util.List;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.vector.ValueVector;

import com.beust.jcommander.internal.Lists;
import com.google.common.base.Preconditions;

public class VectorContainer implements Iterable<VectorWrapper<?>>, VectorAccessible {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(VectorContainer.class);

  private final List<VectorWrapper<?>> wrappers = Lists.newArrayList();
  private final List<VectorWrapper<?>> oldWrappers = Lists.newArrayList();
  private BatchSchema schema;
  private int recordCount = -1;

  public VectorContainer() {
  }

  // public VectorContainer(List<ValueVector> vectors, List<ValueVector[]> hyperVectors) {
  // assert !vectors.isEmpty() || !hyperVectors.isEmpty();
  //
  // addCollection(vectors);
  //
  // for (ValueVector[] vArr : hyperVectors) {
  // add(vArr);
  // }
  // }

  public void addHyperList(List<ValueVector> vectors) {
    schema = null;
    ValueVector[] vv = new ValueVector[vectors.size()];
    for (int i = 0; i < vv.length; i++) {
      vv[i] = vectors.get(i);
    }
    add(vv);
  }

  /**
   * Get a set of transferred clones of this container. Note that this guarantees that the vectors in the cloned
   * container have the same TypedFieldIds as the existing container, allowing interchangeability in generated code. In
   * the case of hyper vectors, this container actually doesn't do a full transfer, rather creating a clone vector
   * wrapper only.
   * 
   * @param incoming
   *          The RecordBatch iterator the contains the batch we should take over.
   * @return A cloned vector container.
   */
  public static VectorContainer getTransferClone(RecordBatch incoming) {
    VectorContainer vc = new VectorContainer();
    for (VectorWrapper<?> w : incoming) {
      vc.cloneAndTransfer(w);
    }
    return vc;
  }

  private void cloneAndTransfer(VectorWrapper<?> wrapper) {
    wrappers.add(wrapper.cloneAndTransfer());
  }

  public void addCollection(Iterable<ValueVector> vectors) {
    schema = null;
    for (ValueVector vv : vectors) {
      wrappers.add(SimpleVectorWrapper.create(vv));
    }
  }

  public TypedFieldId add(ValueVector vv) {
    schema = null;
    int i = wrappers.size();
    wrappers.add(SimpleVectorWrapper.create(vv));
    return new TypedFieldId(vv.getField().getType(), i, false);
  }

  public void add(ValueVector[] hyperVector) {
    assert hyperVector.length != 0;
    schema = null;
    Class<?> clazz = hyperVector[0].getClass();
    ValueVector[] c = (ValueVector[]) Array.newInstance(clazz, hyperVector.length);
    for (int i = 0; i < hyperVector.length; i++) {
      c[i] = hyperVector[i];
    }
    // todo: work with a merged schema.
    wrappers.add(HyperVectorWrapper.create(hyperVector[0].getField(), c));
  }

  public void remove(ValueVector v) {
    schema = null;
    for (Iterator<VectorWrapper<?>> iter = wrappers.iterator(); iter.hasNext();) {
      VectorWrapper<?> w = iter.next();
      if (!w.isHyper() && v == w.getValueVector()) {
        w.clear();
        iter.remove();
        return;
      }
    }

    throw new IllegalStateException("You attempted to remove a vector that didn't exist.");
  }

  public TypedFieldId getValueVectorId(SchemaPath path) {
    for (int i = 0; i < wrappers.size(); i++) {
      VectorWrapper<?> va = wrappers.get(i);
      if (va.getField().matches(path))
        return new TypedFieldId(va.getField().getType(), i, va.isHyper());
    }
    if(path.getRootSegment().isNamed() && path.getRootSegment().getNameSegment().getPath().equals("_MAP") && path.getRootSegment().isLastPath()) throw new UnsupportedOperationException("Drill does not yet support map references.");
    return null;
  }


  @Override
  @SuppressWarnings("unchecked")
  public VectorWrapper<?> getValueAccessorById(int fieldId, Class<?> clazz) {
    VectorWrapper<?> va = wrappers.get(fieldId);
    assert va != null;
    if (va.getVectorClass() != clazz) {
      logger.warn(String.format(
          "Failure while reading vector.  Expected vector class of %s but was holding vector class %s.",
          clazz.getCanonicalName(), va.getVectorClass().getCanonicalName()));
      return null;
    }
    return (VectorWrapper) va;
  }

  public BatchSchema getSchema() {
    Preconditions
        .checkNotNull(schema,
            "Schema is currently null.  You must call buildSchema(SelectionVectorMode) before this container can return a schema.");
    return schema;
  }

  public void buildSchema(SelectionVectorMode mode) {
    SchemaBuilder bldr = BatchSchema.newBuilder().setSelectionVectorMode(mode);
    for (VectorWrapper<?> v : wrappers) {
      bldr.addField(v.getField());
    }
    this.schema = bldr.build();
  }

  @Override
  public Iterator<VectorWrapper<?>> iterator() {
    return wrappers.iterator();
  }

  public void clear() {
    // TODO: figure out a better approach for this.
    // we don't clear schema because we want empty batches to carry previous schema to avoid extra schema update for no
    // data.  
    // schema = null;
    zeroVectors();
    wrappers.clear();
  }

  public void setRecordCount(int recordCount) {
    this.recordCount = recordCount;
  }

  @Override
  public int getRecordCount() {
    Preconditions.checkState(recordCount != -1, "Record count not set for this vector container");
    return recordCount;
  }
  
  public void zeroVectors(){
    for (VectorWrapper<?> w : wrappers) {
      w.clear();
    }
  }
}
