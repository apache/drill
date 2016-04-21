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
import org.apache.arrow.vector.util.BasicTypeHelper;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.proto.UserBitShared.SerializedField;
import org.apache.arrow.vector.types.MaterializedField;
import org.apache.arrow.vector.types.SerializedFieldHelper;
import org.apache.arrow.vector.ValueVector;

import java.util.List;

public class RepeatedMapVectorHelper {
  private RepeatedMapVector repeatedMapVector;

  public RepeatedMapVectorHelper(RepeatedMapVector vector) {
    this.repeatedMapVector = vector;
  }

  public void load(SerializedField metadata, ArrowBuf buffer) {
    final List<SerializedField> children = metadata.getChildList();

    final SerializedField offsetField = children.get(0);
    TypeHelper.load(repeatedMapVector.offsets, offsetField, buffer);
    int bufOffset = offsetField.getBufferLength();

    for (int i = 1; i < children.size(); i++) {
      final SerializedField child = children.get(i);
      final MaterializedField fieldDef = SerializedFieldHelper.create(child);
      ValueVector vector = repeatedMapVector.getChild(fieldDef.getLastName());
      if (vector == null) {
  // if we arrive here, we didn't have a matching vector.
        vector = BasicTypeHelper.getNewVector(fieldDef, repeatedMapVector.allocator);
        repeatedMapVector.putChild(fieldDef.getLastName(), vector);
      }
      final int vectorLength = child.getBufferLength();
      TypeHelper.load(vector, child, buffer.slice(bufOffset, vectorLength));
      bufOffset += vectorLength;
    }

    assert bufOffset == buffer.capacity();
  }


  public SerializedField getMetadata() {
    SerializedField.Builder builder = SerializedFieldHelper.getAsBuilder(repeatedMapVector.getField())
        .setBufferLength(repeatedMapVector.getBufferSize())
//   while we don't need to actually read this on load, we need it to make sure we don't skip deserialization of this vector
        .setValueCount(repeatedMapVector.getAccessor().getValueCount());
    builder.addChild(TypeHelper.getMetadata(repeatedMapVector.offsets));
    for (final ValueVector child : repeatedMapVector.getChildren()) {
      builder.addChild(TypeHelper.getMetadata(child));
    }
    return builder.build();
  }
}
