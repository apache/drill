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
import org.apache.arrow.vector.BaseValueVectorHelper;
import org.apache.arrow.vector.VectorDescriptor;
import org.apache.arrow.vector.complex.BaseRepeatedValueVector;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.common.util.MajorTypeHelper;

public class BaseRepeatedValueVectorHelper extends BaseValueVectorHelper {

  private BaseRepeatedValueVector vector;

  public BaseRepeatedValueVectorHelper(BaseRepeatedValueVector vector) {
    super(vector);
    this.vector = vector;
  }

  public UserBitShared.SerializedField.Builder getMetadataBuilder() {
    return super.getMetadataBuilder()
        .addChild(TypeHelper.getMetadata(vector.offsets))
        .addChild(TypeHelper.getMetadata(vector.vector));
  }

  public void load(UserBitShared.SerializedField metadata, ArrowBuf buffer) {
    final UserBitShared.SerializedField offsetMetadata = metadata.getChild(0);
    TypeHelper.load(vector.offsets, offsetMetadata, buffer);

    final UserBitShared.SerializedField vectorMetadata = metadata.getChild(1);
    if (vector.getDataVector() == BaseRepeatedValueVector.DEFAULT_DATA_VECTOR) {
      vector.addOrGetVector(VectorDescriptor.create(MajorTypeHelper.getArrowMajorType(vectorMetadata.getMajorType())));
    }

    final int offsetLength = offsetMetadata.getBufferLength();
    final int vectorLength = vectorMetadata.getBufferLength();
    TypeHelper.load(vector.vector, vectorMetadata, buffer.slice(offsetLength, vectorLength));
  }
}
