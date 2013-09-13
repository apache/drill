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

import java.util.Iterator;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.record.selection.SelectionVector4;

public abstract class AbstractRecordBatch<T extends PhysicalOperator> implements RecordBatch{
  final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(this.getClass());
  
  protected final VectorContainer container = new VectorContainer();
  protected final T popConfig;
  protected final FragmentContext context;
  
  protected AbstractRecordBatch(T popConfig, FragmentContext context) {
    super();
    this.context = context;
    this.popConfig = popConfig;
  }
  
  @Override
  public Iterator<VectorWrapper<?>> iterator() {
    return container.iterator();
  }

  @Override
  public FragmentContext getContext() {
    return context;
  }

  public PhysicalOperator getPopConfig() {
    return popConfig;
  }

  @Override
  public BatchSchema getSchema() {
    return container.getSchema();
  }

  @Override
  public void kill() {
    container.zeroVectors();
    killIncoming();
    cleanup();
  }
  
  protected abstract void killIncoming();
  
  protected void cleanup(){
  }
  
  @Override
  public SelectionVector2 getSelectionVector2() {
    throw new UnsupportedOperationException();
  }

  @Override
  public SelectionVector4 getSelectionVector4() {
    throw new UnsupportedOperationException();
  }

  @Override
  public TypedFieldId getValueVectorId(SchemaPath path) {
    return container.getValueVectorId(path);
  }

  @Override
  public VectorWrapper<?> getValueAccessorById(int fieldId, Class<?> clazz) {
    return container.getValueAccessorById(fieldId, clazz);
  }

  
  @Override
  public WritableBatch getWritableBatch() {
    return WritableBatch.get(this);
  }
  
  
}
