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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;

import java.util.BitSet;
import java.util.Random;

import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class TestOpenBitSet {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestOpenBitSet.class);

  Random random = new Random();
  ByteBufAllocator allocator = UnpooledByteBufAllocator.DEFAULT;
  
  public int atLeast(int val){
    return val + random.nextInt(val);
  }
  
  
  public Random random() {
    return new Random();
  }

  void doGet(BitSet a, BufBitSet b) {
    int max = a.size();
    for (int i = 0; i < max; i++) {
      if (a.get(i) != b.get(i)) {
        fail("mismatch: BitSet=[" + i + "]=" + a.get(i));
      }
      if (a.get(i) != b.get((long) i)) {
        fail("mismatch: BitSet=[" + i + "]=" + a.get(i));
      }
    }
  }

  void doGetFast(BitSet a, BufBitSet b, int max) {
    for (int i = 0; i < max; i++) {
      if (a.get(i) != b.fastGet(i)) {
        fail("mismatch: BitSet=[" + i + "]=" + a.get(i));
      }
      if (a.get(i) != b.fastGet((long) i)) {
        fail("mismatch: BitSet=[" + i + "]=" + a.get(i));
      }
    }
  }

  void doNextSetBit(BitSet a, BufBitSet b) {
    int aa = -1, bb = -1;
    do {
      aa = a.nextSetBit(aa + 1);
      bb = b.nextSetBit(bb + 1);
      assertEquals(aa, bb);
    } while (aa >= 0);
  }

  void doNextSetBitLong(BitSet a, BufBitSet b) {
    int aa = -1, bb = -1;
    do {
      aa = a.nextSetBit(aa + 1);
      bb = (int) b.nextSetBit((long) (bb + 1));
      assertEquals(aa, bb);
    } while (aa >= 0);
  }

  void doPrevSetBit(BitSet a, BufBitSet b) {
    int aa = a.size() + random().nextInt(100);
    int bb = aa;
    do {
      // aa = a.prevSetBit(aa-1);
      aa--;
      while ((aa >= 0) && (!a.get(aa))) {
        aa--;
      }
      bb = b.prevSetBit(bb - 1);
      assertEquals(aa, bb);
    } while (aa >= 0);
  }

  void doPrevSetBitLong(BitSet a, BufBitSet b) {
    int aa = a.size() + random().nextInt(100);
    int bb = aa;
    do {
      // aa = a.prevSetBit(aa-1);
      aa--;
      while ((aa >= 0) && (!a.get(aa))) {
        aa--;
      }
      bb = (int) b.prevSetBit((long) (bb - 1));
      assertEquals(aa, bb);
    } while (aa >= 0);
  }

  // test interleaving different OpenBitSetIterator.next()/skipTo()
  void doIterate(BitSet a, BufBitSet b, int mode) {
    // if (mode == 1) doIterate1(a, b);
    // if (mode == 2) doIterate2(a, b);
  }

  //
  // void doIterate1(BitSet a, OpenBitSet b) {
  // int aa = -1, bb = -1;
  // OpenBitSetIterator iterator = new OpenBitSetIterator(b);
  // do {
  // aa = a.nextSetBit(aa + 1);
  // bb = random().nextBoolean() ? iterator.nextDoc() : iterator.advance(bb + 1);
  // assertEquals(aa == -1 ? DocIdSetIterator.NO_MORE_DOCS : aa, bb);
  // } while (aa >= 0);
  // }
  //
  // void doIterate2(BitSet a, OpenBitSet b) {
  // int aa = -1, bb = -1;
  // OpenBitSetIterator iterator = new OpenBitSetIterator(b);
  // do {
  // aa = a.nextSetBit(aa + 1);
  // bb = random().nextBoolean() ? iterator.nextDoc() : iterator.advance(bb + 1);
  // assertEquals(aa == -1 ? DocIdSetIterator.NO_MORE_DOCS : aa, bb);
  // } while (aa >= 0);
  // }

  void doRandomSets(int maxSize, int iter, int mode) {
    BitSet a0 = null;
    BufBitSet b0 = null;

    for (int i = 0; i < iter; i++) {
      int sz = random().nextInt(maxSize);
      BitSet a = new BitSet(sz);
      BufBitSet b = new BufBitSet(sz, allocator);

      // test the various ways of setting bits
      if (sz > 0) {
        int nOper = random().nextInt(sz);
        for (int j = 0; j < nOper; j++) {
          int idx;

          idx = random().nextInt(sz);
          a.set(idx);
          b.fastSet(idx);

          idx = random().nextInt(sz);
          a.set(idx);
          b.fastSet((long) idx);

          idx = random().nextInt(sz);
          a.clear(idx);
          b.fastClear(idx);

          idx = random().nextInt(sz);
          a.clear(idx);
          b.fastClear((long) idx);

          idx = random().nextInt(sz);
          a.flip(idx);
          b.fastFlip(idx);

          boolean val = b.flipAndGet(idx);
          boolean val2 = b.flipAndGet(idx);
          assertTrue(val != val2);

          idx = random().nextInt(sz);
          a.flip(idx);
          b.fastFlip((long) idx);

          val = b.flipAndGet((long) idx);
          val2 = b.flipAndGet((long) idx);
          assertTrue(val != val2);

          val = b.getAndSet(idx);
          assertTrue(val2 == val);
          assertTrue(b.get(idx));

          if (!val) b.fastClear(idx);
          assertTrue(b.get(idx) == val);
        }
      }

      // test that the various ways of accessing the bits are equivalent
      doGet(a, b);
      doGetFast(a, b, sz);

      // test ranges, including possible extension
      int fromIndex, toIndex;
      fromIndex = random().nextInt(sz + 80);
      toIndex = fromIndex + random().nextInt((sz >> 1) + 1);
      BitSet aa = (BitSet) a.clone();
      aa.flip(fromIndex, toIndex);
      BufBitSet bb = b.cloneTest();
      bb.flip(fromIndex, toIndex);

      doIterate(aa, bb, mode); // a problem here is from flip or doIterate

      fromIndex = random().nextInt(sz + 80);
      toIndex = fromIndex + random().nextInt((sz >> 1) + 1);
      aa = (BitSet) a.clone();
      aa.clear(fromIndex, toIndex);
      bb = b.cloneTest();
      bb.clear(fromIndex, toIndex);

      doNextSetBit(aa, bb); // a problem here is from clear() or nextSetBit
      doNextSetBitLong(aa, bb);

      doPrevSetBit(aa, bb);
      doPrevSetBitLong(aa, bb);

      fromIndex = random().nextInt(sz + 80);
      toIndex = fromIndex + random().nextInt((sz >> 1) + 1);
      aa = (BitSet) a.clone();
      aa.set(fromIndex, toIndex);
      bb = b.cloneTest();
      bb.set(fromIndex, toIndex);

      doNextSetBit(aa, bb); // a problem here is from set() or nextSetBit
      doNextSetBitLong(aa, bb);

      doPrevSetBit(aa, bb);
      doPrevSetBitLong(aa, bb);

      if (a0 != null) {
        assertEquals(a.equals(a0), b.equals(b0));

        assertEquals(a.cardinality(), b.cardinality());

        BitSet a_and = (BitSet) a.clone();
        a_and.and(a0);
        BitSet a_or = (BitSet) a.clone();
        a_or.or(a0);
        BitSet a_xor = (BitSet) a.clone();
        a_xor.xor(a0);
        BitSet a_andn = (BitSet) a.clone();
        a_andn.andNot(a0);

        BufBitSet b_and = b.cloneTest();
        assertEquals(b, b_and);
        b_and.and(b0);
        BufBitSet b_or = b.cloneTest();
        b_or.or(b0);
        BufBitSet b_xor = b.cloneTest();
        b_xor.xor(b0);
        BufBitSet b_andn = b.cloneTest();
        b_andn.andNot(b0);

        doIterate(a_and, b_and, mode);
        doIterate(a_or, b_or, mode);
        doIterate(a_xor, b_xor, mode);
        doIterate(a_andn, b_andn, mode);

        assertEquals(a_and.cardinality(), b_and.cardinality());
        assertEquals(a_or.cardinality(), b_or.cardinality());
        assertEquals(a_xor.cardinality(), b_xor.cardinality());
        assertEquals(a_andn.cardinality(), b_andn.cardinality());

        // test non-mutating popcounts
        assertEquals(b_and.cardinality(), BufBitSet.intersectionCount(b, b0));
        assertEquals(b_or.cardinality(), BufBitSet.unionCount(b, b0));
        assertEquals(b_xor.cardinality(), BufBitSet.xorCount(b, b0));
        assertEquals(b_andn.cardinality(), BufBitSet.andNotCount(b, b0));
      }

      a0 = a;
      b0 = b;
    }
  }

  // large enough to flush obvious bugs, small enough to run in <.5 sec as part of a
  // larger testsuite.
  @Test
  public void testSmall() {
    doRandomSets(atLeast(1200), atLeast(1000), 1);
    doRandomSets(atLeast(1200), atLeast(1000), 2);
  }

  // uncomment to run a bigger test (~2 minutes).
  /*
   * public void testBig() { doRandomSets(2000,200000, 1); doRandomSets(2000,200000, 2); }
   */

  @Test
  public void testEquals() {
    BufBitSet b1 = new BufBitSet(1111, allocator);
    BufBitSet b2 = new BufBitSet(2222, allocator);
    assertTrue(b1.equals(b2));
    assertTrue(b2.equals(b1));
    b1.set(10);
    assertFalse(b1.equals(b2));
    assertFalse(b2.equals(b1));
    b2.set(10);
    assertTrue(b1.equals(b2));
    assertTrue(b2.equals(b1));
    b2.set(2221);
    assertFalse(b1.equals(b2));
    assertFalse(b2.equals(b1));
    b1.set(2221);
    assertTrue(b1.equals(b2));
    assertTrue(b2.equals(b1));

    // try different type of object
    assertFalse(b1.equals(new Object()));
  }

  @Test
  public void testHashCodeEquals() {
    BufBitSet bs1 = new BufBitSet(200, allocator);
    BufBitSet bs2 = new BufBitSet(64, allocator);
    bs1.set(3);
    bs2.set(3);
    assertEquals(bs1, bs2);
    assertEquals(bs1.hashCode(), bs2.hashCode());
  }

  private BufBitSet makeOpenBitSet(int[] a) {
    BufBitSet bs = new BufBitSet(64, allocator);
    for (int e : a) {
      bs.set(e);
    }
    return bs;
  }

  private BitSet makeBitSet(int[] a) {
    BitSet bs = new BitSet();
    for (int e : a) {
      bs.set(e);
    }
    return bs;
  }

  private void checkPrevSetBitArray(int[] a) {
    BufBitSet obs = makeOpenBitSet(a);
    BitSet bs = makeBitSet(a);
    doPrevSetBit(bs, obs);
  }

  public void testPrevSetBit() {
    checkPrevSetBitArray(new int[] {});
    checkPrevSetBitArray(new int[] { 0 });
    checkPrevSetBitArray(new int[] { 0, 2 });
  }
}
