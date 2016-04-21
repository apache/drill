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
import org.apache.drill.exec.proto.UserBitShared.SerializedField;

public class RepeatedListVectorHelper {
  private RepeatedListVector repeatedListVector;

  public RepeatedListVectorHelper(RepeatedListVector vector) {
    this.repeatedListVector = vector;
  }

  public void load(SerializedField metadata, ArrowBuf buf) {
    new DelegateRepeatedVectorHelper(repeatedListVector.delegate).load(metadata, buf);
  }

  public SerializedField getMetadata() {
    return new DelegateRepeatedVectorHelper(repeatedListVector.delegate).getMetadata();
  }

  private static class DelegateRepeatedVectorHelper extends BaseRepeatedValueVectorHelper {

    private BaseRepeatedValueVector baseRepeatedValueVector;

    public DelegateRepeatedVectorHelper(BaseRepeatedValueVector vector) {
      super(vector);
      this.baseRepeatedValueVector = vector;
    }
  }
}
