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
package org.apache.drill.exec.vector;

public class AllocationHelper {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AllocationHelper.class);
  
  public static void allocate(ValueVector v, int valueCount, int bytesPerValue){
    allocate(v, valueCount, bytesPerValue, 5);
  }
  
  public static void allocate(ValueVector v, int valueCount, int bytesPerValue, int repeatedPerTop){
    if(v instanceof FixedWidthVector){
      ((FixedWidthVector) v).allocateNew(valueCount);
    } else if (v instanceof VariableWidthVector) {
      ((VariableWidthVector) v).allocateNew(valueCount * bytesPerValue, valueCount);
    }else if(v instanceof RepeatedFixedWidthVector){
      ((RepeatedFixedWidthVector) v).allocateNew(valueCount, valueCount * repeatedPerTop);
    }else if(v instanceof RepeatedVariableWidthVector){
      ((RepeatedVariableWidthVector) v).allocateNew(valueCount * bytesPerValue, valueCount, valueCount * repeatedPerTop);
    }else{
      throw new UnsupportedOperationException();
    }
  }
}
