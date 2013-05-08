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
package org.apache.drill.exec.ops.filter;

import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.memory.DirectBufferAllocator;
import org.apache.drill.exec.record.vector.NullableInt32Vector;
import org.apache.drill.exec.record.vector.UInt16Vector;
import org.codehaus.janino.ExpressionEvaluator;

public class SelectionVectorUpdater {
  //static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SelectionVectorUpdater.class);

  // Add a selection vector to a record batch.
  /**
   * where a + b < 10
   */

  public static int applyToBatch(final int recordCount, final NullableInt32Vector a, final NullableInt32Vector b,
      final UInt16Vector selectionVector) {
    int selectionIndex = 0;
    for (int i = 0; i < recordCount; i++) {
      int isNotNull = a.isNull(i) * b.isNull(i);
      if (isNotNull > 0 && a.get(i) + b.get(i) < 10) {
        selectionVector.set(selectionIndex, (char) i);
        selectionIndex++;
      }
    }
    return selectionIndex;
  }

  public static void mai2n(String[] args) {
    int size = 1024;
    BufferAllocator allocator = new DirectBufferAllocator();
    NullableInt32Vector a = new NullableInt32Vector(0, allocator);
    NullableInt32Vector b = new NullableInt32Vector(1, allocator);
    UInt16Vector select = new UInt16Vector(2, allocator);
    a.allocateNew(size);
    b.allocateNew(size);
    select.allocateNew(size);
    int r = 0;
    for (int i = 0; i < 1500; i++) {
      r += applyToBatch(size, a, b, select);
    }

    System.out.println(r);
  }
  
public static void main(String[] args) throws Exception{
  ExpressionEvaluator ee = new ExpressionEvaluator(
      "c > d ? c : d",                     // expression
      int.class,                           // expressionType
      new String[] { "c", "d" },           // parameterNames
      new Class[] { int.class, int.class } // parameterTypes
  );
  
  Integer res = (Integer) ee.evaluate(
      new Object[] {          // parameterValues
          new Integer(10),
          new Integer(11),
      }
  );
  System.out.println("res = " + res);
}
}
