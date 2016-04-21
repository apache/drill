/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.arrow.vector.complex;


import io.netty.buffer.ArrowBuf;
import org.apache.arrow.vector.VectorDescriptor;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.proto.UserBitShared.SerializedField;
import org.apache.arrow.vector.types.SerializedFieldHelper;

import static org.apache.drill.common.util.MajorTypeHelper.getArrowMajorType;

public class ListVectorHelper extends BaseRepeatedValueVectorHelper {
  private ListVector listVector;

  public ListVectorHelper(ListVector vector) {
    super(vector);
    this.listVector = vector;
  }

  public void load(SerializedField metadata, ArrowBuf buffer) {
    final SerializedField offsetMetadata = metadata.getChild(0);
    TypeHelper.load(listVector.offsets, offsetMetadata, buffer);

    final int offsetLength = offsetMetadata.getBufferLength();
    final SerializedField bitMetadata = metadata.getChild(1);
    final int bitLength = bitMetadata.getBufferLength();
    TypeHelper.load(listVector.bits, bitMetadata, buffer.slice(offsetLength, bitLength));

    final SerializedField vectorMetadata = metadata.getChild(2);
    if (listVector.getDataVector() == BaseRepeatedValueVector.DEFAULT_DATA_VECTOR) {
      listVector.addOrGetVector(VectorDescriptor.create(getArrowMajorType(vectorMetadata.getMajorType())));
    }

    final int vectorLength = vectorMetadata.getBufferLength();
    TypeHelper.load(listVector.vector, vectorMetadata, buffer.slice(offsetLength + bitLength, vectorLength));
  }

  public SerializedField.Builder getMetadataBuilder() {
    return SerializedFieldHelper.getAsBuilder(listVector.getField())
            .setValueCount(listVector.getAccessor().getValueCount())
            .setBufferLength(listVector.getBufferSize())
            .addChild(TypeHelper.getMetadata(listVector.offsets))
            .addChild(TypeHelper.getMetadata(listVector.bits))
            .addChild(TypeHelper.getMetadata(listVector.vector));
  }
}
