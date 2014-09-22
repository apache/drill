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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.complex.AbstractMapVector;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class VectorContainer extends AbstractMapVector implements Iterable<VectorWrapper<?>>, VectorAccessible {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(VectorContainer.class);

  protected final List<VectorWrapper<?>> wrappers = Lists.newArrayList();
  private BatchSchema schema;
  private int recordCount = -1;
  private final OperatorContext oContext;

  public VectorContainer() {
    this.oContext = null;
  }

  public VectorContainer( OperatorContext oContext) {
    this.oContext = oContext;
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
    addHyperList(vectors, true);
  }

  public void addHyperList(List<ValueVector> vectors, boolean releasable) {
    schema = null;
    ValueVector[] vv = new ValueVector[vectors.size()];
    for (int i = 0; i < vv.length; i++) {
      vv[i] = vectors.get(i);
    }
    add(vv, releasable);
  }

  public <T extends ValueVector> T addOrGet(String name, MajorType type, Class<T> clazz) {
    MaterializedField field = MaterializedField.create(name, type);
    ValueVector v = TypeHelper.getNewVector(field, this.oContext.getAllocator());

    add(v);

    if (clazz.isAssignableFrom(v.getClass())) {
      return (T) v;
    } else {
      throw new IllegalStateException(String.format("Vector requested [%s] was different than type stored [%s].  Drill doesn't yet support hetergenous types.", clazz.getSimpleName(), v.getClass().getSimpleName()));
    }
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
  public static VectorContainer getTransferClone(VectorAccessible incoming) {
    VectorContainer vc = new VectorContainer();
    for (VectorWrapper<?> w : incoming) {
      vc.cloneAndTransfer(w);
    }
    return vc;
  }

  public static VectorContainer getTransferClone(VectorAccessible incoming, VectorWrapper[] ignoreWrappers) {
    VectorContainer vc = new VectorContainer();
    for (VectorWrapper<?> w : incoming) {
      if(ignoreWrappers != null) {
        for(VectorWrapper wrapper : ignoreWrappers) {
          if (w == wrapper) {
            continue;
          }
        }
      }

      vc.cloneAndTransfer(w);
    }

    return vc;
  }

  public static VectorContainer canonicalize(VectorContainer original) {
    VectorContainer vc = new VectorContainer();
    List<VectorWrapper<?>> canonicalWrappers = new ArrayList<VectorWrapper<?>>(original.wrappers);
    // Sort list of VectorWrapper alphabetically based on SchemaPath.
    Collections.sort(canonicalWrappers, new Comparator<VectorWrapper<?>>() {
      public int compare(VectorWrapper<?> v1, VectorWrapper<?> v2) {
        return v1.getField().getPath().toExpr().compareTo(v2.getField().getPath().toExpr());
      }
    });

    for (VectorWrapper<?> w : canonicalWrappers) {
      vc.add(w.getValueVector());
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
    return new TypedFieldId(vv.getField().getType(), i);
  }

  public void add(ValueVector[] hyperVector) {
    add(hyperVector, true);
  }

  public void add(ValueVector[] hyperVector, boolean releasable) {
    assert hyperVector.length != 0;
    schema = null;
    Class<?> clazz = hyperVector[0].getClass();
    ValueVector[] c = (ValueVector[]) Array.newInstance(clazz, hyperVector.length);
    for (int i = 0; i < hyperVector.length; i++) {
      c[i] = hyperVector[i];
    }
    // todo: work with a merged schema.
    wrappers.add(HyperVectorWrapper.create(hyperVector[0].getField(), c, releasable));
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
      TypedFieldId id = va.getFieldIdIfMatches(i, path);
      if (id != null) {
        return id;
      }
    }

    return null;
  }

  @Override
  public VectorWrapper<?> getValueAccessorById(Class<?> clazz, int... fieldIds) {
    Preconditions.checkArgument(fieldIds.length >= 1);
    VectorWrapper<?> va = wrappers.get(fieldIds[0]);

    if (va == null) {
      return null;
    }

    if (fieldIds.length == 1 && clazz != null && !clazz.isAssignableFrom(va.getVectorClass())) {
      throw new IllegalStateException(String.format(
          "Failure while reading vector.  Expected vector class of %s but was holding vector class %s.",
          clazz.getCanonicalName(), va.getVectorClass().getCanonicalName()));
    }

    return va.getChildWrapper(fieldIds);

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
    schema = null;
    zeroVectors();
    wrappers.clear();
  }

  public void setRecordCount(int recordCount) {
    this.recordCount = recordCount;
  }

  @Override
  public int getRecordCount() {
    if (recordCount < 0) {
      System.out.println();
    }
    Preconditions.checkState(recordCount != -1, "Record count not set for this vector container");
    return recordCount;
  }

  public void zeroVectors() {
    for (VectorWrapper<?> w : wrappers) {
      w.clear();
    }
  }

  public int getNumberOfColumns() {
    return this.wrappers.size();
  }

  public void allocateNew() {
    for (VectorWrapper<?> w : wrappers) {
      w.getValueVector().allocateNew();
    }
  }

  public boolean allocateNewSafe() {
    for (VectorWrapper<?> w : wrappers) {
      if (!w.getValueVector().allocateNewSafe()) {
        return false;
      }
    }
    return true;
  }

}
