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
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.MaterializedField;
import org.apache.arrow.vector.util.BasicTypeHelper;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.proto.UserBitShared.SerializedField;
import org.apache.arrow.vector.types.SerializedFieldHelper;

import java.util.List;

public class MapVectorHelper {
  private MapVector mapVector;

  public MapVectorHelper(MapVector vector) {
    this.mapVector = vector;
  }

  public void load(SerializedField metadata, ArrowBuf buf) {
    final List<SerializedField> fields = metadata.getChildList();
    mapVector.valueCount = metadata.getValueCount();

    int bufOffset = 0;
    for (final SerializedField child : fields) {
      final MaterializedField fieldDef = SerializedFieldHelper.create(child);

      ValueVector vector = mapVector.getChild(fieldDef.getLastName());
      if (vector == null) {
//         if we arrive here, we didn't have a matching vector.
        vector = BasicTypeHelper.getNewVector(fieldDef, mapVector.allocator);
        mapVector.putChild(fieldDef.getLastName(), vector);
      }
      if (child.getValueCount() == 0) {
        vector.clear();
      } else {
        TypeHelper.load(vector, child, buf.slice(bufOffset, child.getBufferLength()));
      }
      bufOffset += child.getBufferLength();
    }

    assert bufOffset == buf.capacity();
  }

  public SerializedField getMetadata() {
    SerializedField.Builder b = SerializedFieldHelper.getAsBuilder(mapVector.getField())
        .setBufferLength(mapVector.getBufferSize())
        .setValueCount(mapVector.valueCount);


    for(ValueVector v : mapVector.getChildren()) {
      b.addChild(TypeHelper.getMetadata(v));
    }
    return b.build();
  }
}
