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
package org.apache.drill.exec.record.vector;

import org.apache.drill.common.expression.types.DataType;
import org.apache.drill.common.physical.RecordField.ValueMode;
import org.apache.drill.exec.BufferAllocator;
import org.apache.drill.exec.record.MaterializedField;

/**
 * Describes a vector which holds a number of true/false values.
 */
public class BitVector extends AbstractFixedValueVector<BitVector> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BitVector.class);

  private final MaterializedField field;
  
  public BitVector(int fieldId, BufferAllocator allocator) {
    super(fieldId, allocator, 1);
    this.field = new MaterializedField(fieldId, DataType.BOOLEAN, false, ValueMode.VECTOR, this.getClass());
  }

  @Override
  public MaterializedField getField() {
    return field;
  }
  
  /** Returns true or false for the specified bit index.
   * The index should be less than the OpenBitSet size
   */
  public boolean get(int index) {
    assert index >= 0 && index < this.valueCount;
    int i = index >> 3;               // div 8
    // signed shift will keep a negative index and force an
    // array-index-out-of-bounds-exception, removing the need for an explicit check.
    int bit = index & 0x3f;           // mod 64
    long bitmask = 1L << bit;
    return (data.getLong(i) & bitmask) != 0;
  }
  
  /** Sets the bit at the specified index.
   * The index should be less than the OpenBitSet size.
   */
   public void set(int index) {
     assert index >= 0 && index < this.valueCount;
     int wordNum = index >> 3;   
     int bit = index & 0x3f;
     long bitmask = 1L << bit;
     data.setLong(wordNum, data.getLong(wordNum) | bitmask);
   }
   
   public void clear(int index) {
     assert index >= 0 && index < this.valueCount;
     int wordNum = index >> 3;
     int bit = index & 0x03f;
     long bitmask = 1L << bit;
     data.setLong(wordNum, data.getLong(wordNum) & ~bitmask);
   }
   
   
   /** Clears a range of bits.  Clearing past the end does not change the size of the set.
   *
   * @param startIndex lower index
   * @param endIndex one-past the last bit to clear
   */
  private void clear(int startIndex, int endIndex) {
    if (endIndex <= startIndex) return;

    int startWord = (startIndex>>3);
    if (startWord >= this.longWords) return;

    // since endIndex is one past the end, this is index of the last
    // word to be changed.
    int endWord   = ((endIndex-1)>>3);

    long startmask = -1L << startIndex;
    long endmask = -1L >>> -endIndex;  // 64-(endIndex&0x3f) is the same as -endIndex due to wrap

    // invert masks since we are clearing
    startmask = ~startmask;
    endmask = ~endmask;

    if (startWord == endWord) {
      data.setLong(startWord,  data.getLong(startWord) & (startmask | endmask));
      return;
    }

    data.setLong(startWord,  data.getLong(startWord) & startmask);

    int middle = Math.min(this.longWords, endWord);
    for(int i =startWord+1; i < middle; i += 8){
      data.setLong(i, 0L);
    }
    if (endWord < this.longWords) {
      data.setLong(endWord,  data.getLong(endWord) & endmask);
    }
  }
  
  public void setAllFalse(){
    clear(0, valueCount);
  }

}
