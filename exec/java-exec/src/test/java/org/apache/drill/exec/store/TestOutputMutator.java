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
package org.apache.drill.exec.store;

import java.util.Iterator;
import java.util.Map;

import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.vector.ValueVector;

import com.google.common.collect.Maps;

public class TestOutputMutator implements OutputMutator, Iterable<VectorWrapper<?>> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestOutputMutator.class);

  private final VectorContainer container = new VectorContainer();
  private final Map<MaterializedField, ValueVector> fieldVectorMap = Maps.newHashMap();

  public void removeField(MaterializedField field) throws SchemaChangeException {
    ValueVector vector = fieldVectorMap.remove(field);
    if (vector == null)
      throw new SchemaChangeException("Failure attempting to remove an unknown field.");
    container.remove(vector);
    vector.close();
  }

  public void addField(ValueVector vector) {
    container.add(vector);
    fieldVectorMap.put(vector.getField(), vector);
  }

  @Override
  public void removeAllFields() {
    for (VectorWrapper<?> vw : container) {
      vw.clear();
    }
    container.clear();
    fieldVectorMap.clear();
  }

  @Override
  public void setNewSchema() throws SchemaChangeException {
    container.buildSchema(SelectionVectorMode.NONE);
  }

  public Iterator<VectorWrapper<?>> iterator() {
    return container.iterator();
  }

  public void clear(){
    removeAllFields();
  }

}
