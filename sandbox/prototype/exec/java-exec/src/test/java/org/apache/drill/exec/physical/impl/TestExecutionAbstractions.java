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

package org.apache.drill.exec.physical.impl;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.Random;

import sun.misc.Unsafe;
import com.google.caliper.Benchmark;
import com.google.caliper.Param;

@SuppressWarnings("restriction")
public class TestExecutionAbstractions extends Benchmark {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestExecutionAbstractions.class);

  /**
   * General goal is compare the performance of abstract versus concrete
   * implementations of selection vector dereferencing.
   */

  private static enum Implementation {
    CONCRETE, ABSTRACT
  };

  private static enum SelectionVectorMode {
    NONE, SV2, SV4
  };

  @Param
  private Implementation impl;
  @Param
  private SelectionVectorMode mode;

  private int scale = 1024*1024*8;

  private final Unsafe unsafe = retrieveUnsafe();
  private final ByteBuffer a;
  private final ByteBuffer b;
  private final ByteBuffer b2;
  private final ByteBuffer c;
  private final ByteBuffer sv2;
  private final ByteBuffer sv4;
  private final int max;
  

  public TestExecutionAbstractions() throws Exception {
    sv2 = ByteBuffer.allocateDirect(scale * 2);
    sv4 = ByteBuffer.allocateDirect(scale * 4);
    a = ByteBuffer.allocateDirect(scale * 8);
    b = ByteBuffer.allocateDirect(scale * 8);
    b2 = ByteBuffer.allocateDirect(scale * 8);
    c = ByteBuffer.allocateDirect(scale * 8);
    int svPos = 0;
    int i = 0;
    try {

      Random r = new Random();
      for (; i < scale; i++) {
        a.putLong(i * 8, r.nextLong());
        b.putLong(i * 8, r.nextLong());

        if (r.nextBoolean()) {
          sv2.putChar(svPos * 2, (char) i);
          sv4.putInt(svPos * 4, i);
          svPos++;
        }
      }
      System.out.println("Created test data.");
      max = mode == SelectionVectorMode.NONE ? 1024 : svPos;

    } catch (Exception ex) {
      System.out.println("i: " + i + ", svPos" + svPos);
      throw ex;
    }
  }

   private Unsafe retrieveUnsafe(){
     sun.misc.Unsafe localUnsafe = null;
  
   try {
   Field field = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
   field.setAccessible(true);
   localUnsafe = (sun.misc.Unsafe) field.get(null);
   } catch (Exception e) {
   throw new AssertionError(e);
   }
  
   return localUnsafe;
   }

  public void timeAdd(int reps) {
    for (int r = 0; r < reps; r++) {
      switch (impl) {

      case CONCRETE:
        switch (mode) {

        case NONE:
          for (int i = 0; i < max; i++) {
            
            c.putLong(i * 8, a.getLong(i * 8) + b.getLong(i * 8));
          }

          break;
        case SV2:
          for (int i = 0; i < max; i++) {
            int index = sv2.getChar(i*2) * 8;
            c.putLong(i * 8, a.getLong(index) + b.getLong(index));
          }
          break;
        case SV4:
          for (int i = 0; i < max; i++) {
            int index = sv4.getInt(i*4) * 8;
            c.putLong(i * 8, a.getLong(index) + b.getLong(index));
          }
          break;
        }
        break;
      case ABSTRACT:
        LongGetter aGetter = null;
        LongGetter bGetter = null;

        switch (mode) {

        case NONE:
          aGetter = new StraightGetter(a);
          bGetter = new StraightGetter(b);
          break;
        case SV2:
          aGetter = new Sv2Getter(sv2, a);
          bGetter = new Sv2Getter(sv2, b);
          break;
        case SV4:
          aGetter = new Sv4Getter(sv4, a);
          bGetter = new Sv4Getter(sv4, b);
          break;

        }

        for (int i = 0; i < max; i++) {
          c.putLong(i * 8, aGetter.getLong(i) + bGetter.getLong(i));
        }
        break;
      }
    }

  }

  private static interface LongGetter {
    long getLong(int index);
  }

  private static class StraightGetter implements LongGetter {

    final ByteBuffer b;

    public StraightGetter(ByteBuffer b) {
      super();
      this.b = b;
    }

    @Override
    public long getLong(int index) {
      return b.getLong(index * 8);
    }
  }

  private static class Sv2Getter implements LongGetter {
    final ByteBuffer b;
    final ByteBuffer sv;

    public Sv2Getter(ByteBuffer sv, ByteBuffer b) {
      super();
      this.b = b;
      this.sv = sv;
    }

    @Override
    public long getLong(int index) {
      int pos = sv.getChar(index * 2);
      return b.getLong(pos * 8);
    }
  }

  private static class Sv4Getter implements LongGetter {
    final ByteBuffer b;
    final ByteBuffer sv;

    public Sv4Getter(ByteBuffer sv, ByteBuffer b) {
      super();
      this.b = b;
      this.sv = sv;
    }

    @Override
    public long getLong(int index) {
      int pos = sv.getInt(index * 4);
      return b.getLong(pos * 8);
    }
  }
  
  private long allocate(long bytes){
    return unsafe.allocateMemory(bytes);
    
  }
  
  
  
}
