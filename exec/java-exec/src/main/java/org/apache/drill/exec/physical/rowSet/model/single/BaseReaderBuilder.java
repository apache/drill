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
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.rowSet.model.AbstractReaderBuilder;
import org.apache.drill.exec.physical.rowSet.model.MetadataProvider;
import org.apache.drill.exec.physical.rowSet.model.MetadataProvider.VectorDescrip;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.accessor.reader.AbstractObjectReader;
import org.apache.drill.exec.vector.accessor.reader.AbstractScalarReader;
import org.apache.drill.exec.vector.accessor.reader.ArrayReaderImpl;
import org.apache.drill.exec.vector.accessor.reader.MapReader;
import org.apache.drill.exec.vector.accessor.reader.UnionReaderImpl;
import org.apache.drill.exec.vector.accessor.reader.VectorAccessor;
import org.apache.drill.exec.vector.accessor.reader.VectorAccessors.SingleVectorAccessor;
import org.apache.drill.exec.vector.complex.AbstractMapVector;
import org.apache.drill.exec.vector.complex.ListVector;
import org.apache.drill.exec.vector.complex.RepeatedListVector;
import org.apache.drill.exec.vector.complex.UnionVector;

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
    case UNION:
      return buildUnion((UnionVector) vector, va, descrip);
    case LIST:
      return buildList(vector, va, descrip);
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

  private AbstractObjectReader buildUnion(UnionVector vector, VectorAccessor unionAccessor, VectorDescrip descrip) {
    MetadataProvider provider = descrip.childProvider();
    final AbstractObjectReader variants[] = new AbstractObjectReader[MinorType.values().length];
    int i = 0;
    for (MinorType type : vector.getField().getType().getSubTypeList()) {

      // This call will create the vector if it does not yet exist.
      // Will throw an exception for unsupported types.
      // so call this only if the MajorType reports that the type
      // already exists.

      ValueVector memberVector = vector.getMember(type);
      VectorDescrip memberDescrip = new VectorDescrip(provider, i++, memberVector.getField());
      variants[type.ordinal()] = buildVectorReader(memberVector, memberDescrip);
    }
    return UnionReaderImpl.build(
        descrip.metadata,
        unionAccessor,
        variants);
  }

  private AbstractObjectReader buildList(ValueVector vector, VectorAccessor listAccessor,
      VectorDescrip listDescrip) {
    if (vector.getField().getType().getMode() == DataMode.REPEATED) {
      return buildMultiDList((RepeatedListVector) vector, listAccessor, listDescrip);
    } else {
      return build1DList((ListVector) vector, listAccessor, listDescrip);
    }
  }

  private AbstractObjectReader buildMultiDList(RepeatedListVector vector,
      VectorAccessor listAccessor, VectorDescrip listDescrip) {

    ValueVector child = vector.getDataVector();
    if (child == null) {
      throw new UnsupportedOperationException("No child vector for repeated list.");
    }
    VectorDescrip childDescrip = new VectorDescrip(listDescrip.childProvider(), 0, child.getField());
    AbstractObjectReader elementReader = buildVectorReader(child, childDescrip);
    return ArrayReaderImpl.buildRepeatedList(listDescrip.metadata, listAccessor, elementReader);
  }

  /**
   * Build a list vector.
   * <p>
   * The list vector is a complex, somewhat ad-hoc structure. It can
   * take the place of repeated vectors, with some extra features.
   * The four "modes" of list vector, and thus list reader, are:
   * <ul>
   * <li>Similar to a scalar array.</li>
   * <li>Similar to a map (tuple) array.</li>
   * <li>The only way to represent an array of unions.</li>
   * <li>The only way to represent an array of lists.</li>
   * </ul>
   * Lists add an extra feature compared to the "regular" scalar or
   * map arrays. Each array entry can be either null or empty (regular
   * arrays can only be empty.)
   * <p>
   * When working with unions, this introduces an ambiguity: both the
   * list and the union have a null flag. Here, we assume that the
   * list flag has precedence, and that if the list entry is not null
   * then the union must also be not null. (Experience will show whether
   * existing code does, in fact, follow that convention.)
   */

  private AbstractObjectReader build1DList(ListVector vector, VectorAccessor listAccessor,
      VectorDescrip listDescrip) {
    ValueVector dataVector = vector.getDataVector();
    VectorDescrip dataMetadata;
    if (dataVector.getField().getType().getMinorType() == MinorType.UNION) {

      // At the metadata level, a list always holds a union. But, at the
      // implementation layer, a union of a single type is collapsed out
      // to leave just a list of that single type.

      dataMetadata = listDescrip;
    } else {
      dataMetadata = new VectorDescrip(listDescrip.childProvider(), 0, dataVector.getField());
    }
    return ArrayReaderImpl.buildList(listDescrip.metadata,
        listAccessor, buildVectorReader(dataVector, dataMetadata));
  }
}
