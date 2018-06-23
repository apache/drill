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
package org.apache.drill.exec.physical.rowSet.model.single;

import java.util.ArrayList;
import java.util.List;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.exec.physical.rowSet.model.AbstractReaderBuilder;
import org.apache.drill.exec.physical.rowSet.model.MetadataProvider;
import org.apache.drill.exec.physical.rowSet.model.MetadataProvider.VectorDescrip;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.accessor.reader.AbstractObjectReader;
import org.apache.drill.exec.vector.accessor.reader.AbstractScalarReader;
import org.apache.drill.exec.vector.accessor.reader.ArrayReaderImpl;
import org.apache.drill.exec.vector.accessor.reader.MapReader;
import org.apache.drill.exec.vector.accessor.reader.VectorAccessor;
import org.apache.drill.exec.vector.accessor.reader.VectorAccessors.SingleVectorAccessor;
import org.apache.drill.exec.vector.complex.AbstractMapVector;

public abstract class BaseReaderBuilder extends AbstractReaderBuilder {

  protected List<AbstractObjectReader> buildContainerChildren(
      VectorContainer container, MetadataProvider mdProvider) {
    List<AbstractObjectReader> readers = new ArrayList<>();
    for (int i = 0; i < container.getNumberOfColumns(); i++) {
      ValueVector vector = container.getValueVector(i).getValueVector();
      VectorDescrip descrip = new VectorDescrip(mdProvider, i, vector.getField());
      readers.add(buildVectorReader(vector, descrip));
    }
    return readers;
  }

  protected AbstractObjectReader buildVectorReader(ValueVector vector, VectorDescrip descrip) {
    VectorAccessor va = new SingleVectorAccessor(vector);
    MajorType type = va.type();

    switch(type.getMinorType()) {
    case MAP:
      return buildMap((AbstractMapVector) vector, va, type.getMode(), descrip);
    case LATE:

      // Occurs for a list with no type: a list of nulls.

      return AbstractScalarReader.nullReader(descrip.metadata);
    default:
      return buildScalarReader(va, descrip.metadata);
    }
  }

  private AbstractObjectReader buildMap(AbstractMapVector vector, VectorAccessor va, DataMode mode, VectorDescrip descrip) {

    boolean isArray = mode == DataMode.REPEATED;

    // Map type

    AbstractObjectReader mapReader = MapReader.build(
        descrip.metadata,
        isArray ? null : va,
        buildMapMembers(vector,
            descrip.parent.childProvider(descrip.metadata)));

    // Single map

    if (! isArray) {
      return mapReader;
    }

    // Repeated map

    return ArrayReaderImpl.buildTuple(descrip.metadata, va, mapReader);
  }

  protected List<AbstractObjectReader> buildMapMembers(AbstractMapVector mapVector, MetadataProvider provider) {
    List<AbstractObjectReader> readers = new ArrayList<>();
    int i = 0;
    for (ValueVector vector : mapVector) {
      VectorDescrip descrip = new VectorDescrip(provider, i, vector.getField());
      readers.add(buildVectorReader(vector, descrip));
      i++;
    }
    return readers;
  }

}
