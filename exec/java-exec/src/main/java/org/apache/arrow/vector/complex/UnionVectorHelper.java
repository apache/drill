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
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.proto.UserBitShared.SerializedField;
import org.apache.arrow.vector.types.SerializedFieldHelper;

public class UnionVectorHelper {
  private UnionVector unionVector;

  public UnionVectorHelper(UnionVector vector) {
    this.unionVector = vector;
  }

  public void load(UserBitShared.SerializedField metadata, ArrowBuf buffer) {
    unionVector.valueCount = metadata.getValueCount();

    TypeHelper.load(unionVector.internalMap, metadata.getChild(0), buffer);
  }

  public SerializedField getMetadata() {
    SerializedField.Builder b = SerializedFieldHelper.getAsBuilder(unionVector.getField())
            .setBufferLength(unionVector.getBufferSize())
            .setValueCount(unionVector.valueCount);

    b.addChild(TypeHelper.getMetadata(unionVector.internalMap));
    return b.build();
  }

}
