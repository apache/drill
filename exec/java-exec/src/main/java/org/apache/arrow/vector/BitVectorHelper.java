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
package org.apache.arrow.vector;

import com.google.common.base.Preconditions;
import io.netty.buffer.ArrowBuf;
import org.apache.drill.exec.proto.UserBitShared.SerializedField;

public class BitVectorHelper extends BaseValueVectorHelper {

  private BitVector vector;

  public BitVectorHelper(BitVector vector) {
    super(vector);
    this.vector = vector;
  }

  public void load(SerializedField metadata, ArrowBuf buffer) {
    Preconditions.checkArgument(vector.field.getPath().equals(metadata.getNamePart().getName()), "The field %s doesn't match the provided metadata %s.", vector.field, metadata);
    final int valueCount = metadata.getValueCount();
    final int expectedLength = vector.getSizeFromCount(valueCount);
    final int actualLength = metadata.getBufferLength();
    assert expectedLength == actualLength: "expected and actual buffer sizes do not match";

    vector.clear();
    vector.data = buffer.slice(0, actualLength);
    vector.data.retain();
    vector.valueCount = valueCount;
  }
}
