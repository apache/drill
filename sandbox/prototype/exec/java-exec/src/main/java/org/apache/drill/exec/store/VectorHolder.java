/*******************************************************************************
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
 ******************************************************************************/

package org.apache.drill.exec.store;

import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.vector.TypeHelper;
import org.apache.drill.exec.record.vector.ValueVector;

public class VectorHolder {
    private int length;
    private ValueVector.Base vector;
    private int currentLength;

    VectorHolder(int length, ValueVector.Base vector) {
        this.length = length;
        this.vector = vector;
    }

    public ValueVector.Base getValueVector() {
        return vector;
    }

    public void incAndCheckLength(int newLength) {
        if (!hasEnoughSpace(newLength)) {
            throw new BatchExceededException(length, currentLength + newLength);
        }
        currentLength += newLength;
    }

    public boolean hasEnoughSpace(int newLength) {
        return length >= currentLength + newLength;
    }

    public int getLength() {
        return length;
    }

    public void reset() {
        currentLength = 0;
        vector.allocateNew(length);
    }
}
