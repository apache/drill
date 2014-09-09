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

import org.apache.drill.exec.vector.AllocationHelper;
import org.apache.drill.exec.vector.RepeatedFixedWidthVector;
import org.apache.drill.exec.vector.RepeatedMutator;
import org.apache.drill.exec.vector.RepeatedVariableWidthVector;
import org.apache.drill.exec.vector.ValueVector;

public class VectorHolder {
  private int count;
  private int groupCount;
  private int length;
  private ValueVector vector;
  private int currentLength;
  private boolean repeated;


  public VectorHolder(int length, ValueVector vector) {
    this.length = length;
    this.vector = vector;
    if (vector instanceof RepeatedFixedWidthVector || vector instanceof  RepeatedVariableWidthVector) {
      repeated = true;
    }
  }

  public VectorHolder(ValueVector vector) {
    this.length = vector.getValueCapacity();
    this.vector = vector;
    if (vector instanceof RepeatedFixedWidthVector || vector instanceof  RepeatedVariableWidthVector) {
      repeated = true;
    }
  }

  public boolean isRepeated() {
    return repeated;
  }

  public ValueVector getValueVector() {
    return vector;
  }

  public void incAndCheckLength(int newLength) {
    if (!hasEnoughSpace(newLength)) {
      throw new BatchExceededException(length, vector.getBufferSize() + newLength);
    }

    currentLength += newLength;
    count += 1;
  }

  public void setGroupCount(int groupCount) {
    if (this.groupCount < groupCount) {
      RepeatedMutator mutator = (RepeatedMutator) vector.getMutator();
      while (this.groupCount < groupCount) {
        mutator.startNewGroup(++this.groupCount);
      }
    }
  }

  public boolean hasEnoughSpace(int newLength) {
    return length >= currentLength + newLength;
  }

  public int getLength() {
    return length;
  }

  public void reset() {
    currentLength = 0;
    count = 0;
    allocateNew(length);
  }

  public void populateVectorLength() {
    ValueVector.Mutator mutator = vector.getMutator();
    if (vector instanceof RepeatedFixedWidthVector || vector instanceof RepeatedVariableWidthVector) {
      mutator.setValueCount(groupCount);
    } else {
      mutator.setValueCount(count);
    }
  }

  public void allocateNew(int valueLength) {
    AllocationHelper.allocate(vector, valueLength, 10, 5);
  }

  public void allocateNew(int valueLength, int repeatedPerTop) {
    AllocationHelper.allocate(vector, valueLength, 10, repeatedPerTop);
  }
}
