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
package org.apache.drill.exec.vector.allocator;

import org.apache.drill.exec.vector.FixedWidthVector;
import org.apache.drill.exec.vector.RepeatedVariableWidthVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.VariableWidthVector;

@Deprecated
public abstract class VectorAllocator{
  public abstract void alloc(int recordCount);

//  public static VectorAllocator getAllocator(ValueVector in, ValueVector outgoing){
//    if(outgoing instanceof FixedWidthVector){
//      return new FixedVectorAllocator((FixedWidthVector) outgoing);
//    }else if(outgoing instanceof VariableWidthVector && in instanceof VariableWidthVector){
//      return new VariableVectorAllocator( (VariableWidthVector) in, (VariableWidthVector) outgoing);
//    } else if (outgoing instanceof RepeatedVariableWidthVector && in instanceof RepeatedVariableWidthVector) {
//      return new RepeatedVectorAllocator((RepeatedVariableWidthVector) in, (RepeatedVariableWidthVector) outgoing);
//    }else{
//      throw new UnsupportedOperationException();
//    }
//  }

  @Deprecated
  public static VectorAllocator getAllocator(ValueVector outgoing, int averageBytesPerVariable){
    if(outgoing instanceof FixedWidthVector){
      return new FixedVectorAllocator((FixedWidthVector) outgoing);
    }else if(outgoing instanceof VariableWidthVector){
      return new VariableEstimatedVector( (VariableWidthVector) outgoing, averageBytesPerVariable);
    }else if (outgoing instanceof RepeatedVariableWidthVector) {
      return new RepeatedVariableEstimatedAllocator((RepeatedVariableWidthVector) outgoing, averageBytesPerVariable);
    } else {
      throw new UnsupportedOperationException();
    }
  }
}