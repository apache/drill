/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.drill.exec.record.vector; // from org.apache.solr.util rev 555343

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

/**
 * HEAVY WIP: ONLY PARTIALLY TRANSFERRED TO BUFFER METHODS. STILL NEEDS BIT SHIFT FIXES, GETLONG AND SETLONG updates to
 * fix index postion AND OTHER THINGS.
 * 
 * An "open" BitSet implementation that allows direct access to the array of words storing the bits.
 * <p/>
 * Unlike java.util.bitset, the fact that bits are packed into an array of longs is part of the interface. This allows
 * efficient implementation of other algorithms by someone other than the author. It also allows one to efficiently
 * implement alternate serialization or interchange formats.
 * <p/>
 * <code>BufBitSet</code> is faster than <code>java.util.BitSet</code> in most operations and *much* faster at
 * calculating cardinality of sets and results of set operations. It can also handle sets of larger cardinality (up to
 * 64 * 2**32-1)
 * <p/>
 * The goals of <code>BufBitSet</code> are the fastest implementation possible, and maximum code reuse. Extra safety
 * and encapsulation may always be built on top, but if that's built in, the cost can never be removed (and hence people
 * re-implement their own version in order to get better performance). If you want a "safe", totally encapsulated (and
 * slower and limited) BitSet class, use <code>java.util.BitSet</code>.
 * <p/>
 */

public class BufBitSet {
  private ByteBufAllocator allocator;
  private ByteBuf buf;
  // protected long[] bits;
  protected int wlen; // number of words (elements) used in the array

  // Used only for assert:
  private long numBits;

  // /** Constructs an BufBitSet large enough to hold <code>numBits</code>.
  // */
  // public BufBitSet(long numBits) {
  // this.numBits = numBits;
  // wlen = buf.capacity();
  // }
  //
  // public BufBitSet() {
  // this(64);
  // }

  public BufBitSet(long numBits, ByteBufAllocator allocator) {
    this.allocator = allocator;
    this.numBits = numBits;
    int words = bits2words(numBits);
    this.wlen = words;
    buf = allocator.buffer(wlen);
  }

  private BufBitSet(ByteBufAllocator allocator, ByteBuf buf) {
    this.allocator = allocator;
    this.numBits = buf.capacity() * 8;
    int words = buf.capacity();
    this.wlen = words;
    this.buf = buf;
  }

  /** Returns the current capacity in bits (1 greater than the index of the last bit) */
  public long capacity() {
    return buf.capacity() << 6;
  }

  /**
   * Returns the current capacity of this set. Included for compatibility. This is *not* equal to {@link #cardinality}
   */
  public long size() {
    return capacity();
  }

  public int length() {
    return buf.capacity() << 6;
  }

  /** Returns true if there are no set bits */
  public boolean isEmpty() {
    return cardinality() == 0;
  }

  // /** Expert: returns the long[] storing the bits */
  // public long[] getBits() { return bits; }
  //
  // /** Expert: sets a new long[] to use as the bit storage */
  // public void setBits(long[] bits) { this.bits = bits; }

  /** Expert: gets the number of longs in the array that are in use */
  public int getNumWords() {
    return wlen;
  }

  /** Expert: sets the number of longs in the array that are in use */
  public void setNumWords(int nWords) {
    this.wlen = nWords;
  }

  /** Returns true or false for the specified bit index. */
  public boolean get(int index) {
    int i = index >> 6; // div 64
    // signed shift will keep a negative index and force an
    // array-index-out-of-bounds-exception, removing the need for an explicit check.
    if (i >= buf.capacity()) return false;

    int bit = index & 0x3f; // mod 64
    long bitmask = 1L << bit;
    return (buf.getLong(i) & bitmask) != 0;
  }

  /**
   * Returns true or false for the specified bit index. The index should be less than the BufBitSet size
   */
  public boolean fastGet(int index) {
    assert index >= 0 && index < numBits;
    int i = index >> 6; // div 64
    // signed shift will keep a negative index and force an
    // array-index-out-of-bounds-exception, removing the need for an explicit check.
    int bit = index & 0x3f; // mod 64
    long bitmask = 1L << bit;
    return (buf.getLong(i) & bitmask) != 0;
  }

  /**
   * Returns true or false for the specified bit index
   */
  public boolean get(long index) {
    int i = (int) (index >> 6); // div 64
    if (i >= buf.capacity()) return false;
    int bit = (int) index & 0x3f; // mod 64
    long bitmask = 1L << bit;
    return (buf.getLong(i) & bitmask) != 0;
  }

  /**
   * Returns true or false for the specified bit index. The index should be less than the BufBitSet size.
   */
  public boolean fastGet(long index) {
    assert index >= 0 && index < numBits;
    int i = (int) (index >> 6); // div 64
    int bit = (int) index & 0x3f; // mod 64
    long bitmask = 1L << bit;
    return (buf.getLong(i) & bitmask) != 0;
  }

  /*
   * // alternate implementation of get() public boolean get1(int index) { int i = index >> 6; // div 64 int bit = index
   * & 0x3f; // mod 64 return ((buf.getLong(i)>>>bit) & 0x01) != 0; // this does a long shift and a bittest (on x86) vs
   * // a long shift, and a long AND, (the test for zero is prob a no-op) // testing on a P4 indicates this is slower
   * than (buf.getLong(i) & bitmask) != 0; }
   */

  /**
   * returns 1 if the bit is set, 0 if not. The index should be less than the BufBitSet size
   */
  public int getBit(int index) {
    assert index >= 0 && index < numBits;
    int i = index >> 6; // div 64
    int bit = index & 0x3f; // mod 64
    return ((int) (buf.getLong(i) >>> bit)) & 0x01;
  }

  /*
   * public boolean get2(int index) { int word = index >> 6; // div 64 int bit = index & 0x0000003f; // mod 64 return
   * (buf.getLong(word) << bit) < 0; // hmmm, this would work if bit order were reversed // we could right shift and
   * check for parity bit, if it was available to us. }
   */

  /** sets a bit, expanding the set size if necessary */
  public void set(long index) {
    int wordNum = expandingWordNum(index);
    int bit = (int) index & 0x3f;
    long bitmask = 1L << bit;
    buf.setLong(wordNum, buf.getLong(wordNum) | bitmask);
    buf.setLong(wordNum, buf.getLong(wordNum) | bitmask);
  }

  /**
   * Sets the bit at the specified index. The index should be less than the BufBitSet size.
   */
  public void fastSet(int index) {
    assert index >= 0 && index < numBits;
    int wordNum = index >> 6; // div 64
    int bit = index & 0x3f; // mod 64
    long bitmask = 1L << bit;
    buf.setLong(wordNum, buf.getLong(wordNum) | bitmask);
  }

  /**
   * Sets the bit at the specified index. The index should be less than the BufBitSet size.
   */
  public void fastSet(long index) {
    assert index >= 0 && index < numBits;
    int wordNum = (int) (index >> 6);
    int bit = (int) index & 0x3f;
    long bitmask = 1L << bit;
    buf.setLong(wordNum, buf.getLong(wordNum) | bitmask);
  }

  /**
   * Sets a range of bits, expanding the set size if necessary
   * 
   * @param startIndex
   *          lower index
   * @param endIndex
   *          one-past the last bit to set
   */
  public void set(long startIndex, long endIndex) {
    if (endIndex <= startIndex) return;

    int startWord = (int) (startIndex >> 6);

    // since endIndex is one past the end, this is index of the last
    // word to be changed.
    int endWord = expandingWordNum(endIndex - 1);

    long startmask = -1L << startIndex;
    long endmask = -1L >>> -endIndex; // 64-(endIndex&0x3f) is the same as -endIndex due to wrap

    if (startWord == endWord) {
      buf.setLong(startWord, buf.getLong(startWord) | (startmask & endmask));
      return;
    }
    buf.setLong(startWord, buf.getLong(startWord) | startmask);

    fill(buf, startWord + 1, endWord, -1L);
    buf.setLong(endWord, buf.getLong(endWord) | endmask);
  }

  private void fill(ByteBuf buf, int start, int end, long val) {
    for (int i = 0; i < buf.capacity(); i += 8) {
      buf.setLong(i, val);
    }
  }

  private final void setLongWord(int pos, long value) {
    buf.setLong(pos * 8, value);
  }

  private final long getLongWord(int pos) {
    return buf.getLong(pos * 8);
  }

  protected int expandingWordNum(long index) {
    int wordNum = (int) (index >> 6);
    if (wordNum >= wlen) {
      ensureCapacity(index + 1);
      wlen = wordNum + 1;
    }
    assert (numBits = Math.max(numBits, index + 1)) >= 0;
    return wordNum;
  }

  /**
   * clears a bit. The index should be less than the BufBitSet size.
   */
  public void fastClear(int index) {
    assert index >= 0 && index < numBits;
    int wordNum = index >> 6;
    int bit = index & 0x03f;
    long bitmask = 1L << bit;
    buf.setLong(wordNum, buf.getLong(wordNum) & ~bitmask);
    // hmmm, it takes one more instruction to clear than it does to set... any
    // way to work around this? If there were only 63 bits per word, we could
    // use a right shift of 10111111...111 in binary to position the 0 in the
    // correct place (using sign extension).
    // Could also use Long.rotateRight() or rotateLeft() *if* they were converted
    // by the JVM into a native instruction.
    // buf.getLong(word) &= Long.rotateLeft(0xfffffffe,bit);
  }

  /**
   * clears a bit. The index should be less than the BufBitSet size.
   */
  public void fastClear(long index) {
    assert index >= 0 && index < numBits;
    int wordNum = (int) (index >> 6); // div 64
    int bit = (int) index & 0x3f; // mod 64
    long bitmask = 1L << bit;
    buf.setLong(wordNum, buf.getLong(wordNum) & ~bitmask);
  }

  /** clears a bit, allowing access beyond the current set size without changing the size. */
  public void clear(long index) {
    int wordNum = (int) (index >> 6); // div 64
    if (wordNum >= wlen) return;
    int bit = (int) index & 0x3f; // mod 64
    long bitmask = 1L << bit;
    buf.setLong(wordNum, buf.getLong(wordNum) & ~bitmask);
  }

  /**
   * Clears a range of bits. Clearing past the end does not change the size of the set.
   * 
   * @param startIndex
   *          lower index
   * @param endIndex
   *          one-past the last bit to clear
   */
  public void clear(int startIndex, int endIndex) {
    if (endIndex <= startIndex) return;

    int startWord = (startIndex >> 6);
    if (startWord >= wlen) return;

    // since endIndex is one past the end, this is index of the last
    // word to be changed.
    int endWord = ((endIndex - 1) >> 6);

    long startmask = -1L << startIndex;
    long endmask = -1L >>> -endIndex; // 64-(endIndex&0x3f) is the same as -endIndex due to wrap

    // invert masks since we are clearing
    startmask = ~startmask;
    endmask = ~endmask;

    if (startWord == endWord) {
      buf.setLong(startWord, buf.getLong(startWord) & (startmask | endmask));
      return;
    }

    buf.setLong(startWord, buf.getLong(startWord) & startmask);

    int middle = Math.min(wlen, endWord);
    fill(buf, startWord + 1, middle, 0L);
    if (endWord < wlen) {
      buf.setLong(endWord, buf.getLong(endWord) & endmask);
    }
  }

  /**
   * Clears a range of bits. Clearing past the end does not change the size of the set.
   * 
   * @param startIndex
   *          lower index
   * @param endIndex
   *          one-past the last bit to clear
   */
  public void clear(long startIndex, long endIndex) {
    if (endIndex <= startIndex) return;

    int startWord = (int) (startIndex >> 6);
    if (startWord >= wlen) return;

    // since endIndex is one past the end, this is index of the last
    // word to be changed.
    int endWord = (int) ((endIndex - 1) >> 6);

    long startmask = -1L << startIndex;
    long endmask = -1L >>> -endIndex; // 64-(endIndex&0x3f) is the same as -endIndex due to wrap

    // invert masks since we are clearing
    startmask = ~startmask;
    endmask = ~endmask;

    if (startWord == endWord) {
      buf.setLong(startWord, buf.getLong(startWord) & (startmask | endmask));
      return;
    }

    buf.setLong(startWord, buf.getLong(startWord) & startmask);

    int middle = Math.min(wlen, endWord);
    fill(buf, startWord + 1, middle, 0L);
    if (endWord < wlen) {
      buf.setLong(endWord, buf.getLong(endWord) & endmask);
    }
  }

  /**
   * Sets a bit and returns the previous value. The index should be less than the BufBitSet size.
   */
  public boolean getAndSet(int index) {
    assert index >= 0 && index < numBits;
    int wordNum = index >> 6; // div 64
    int bit = index & 0x3f; // mod 64
    long bitmask = 1L << bit;
    long longVal = buf.getLong(wordNum);
    boolean val = (longVal & bitmask) != 0;
    buf.setLong(wordNum, longVal | bitmask);
    return val;
  }

  /**
   * flips a bit. The index should be less than the BufBitSet size.
   */
  public void fastFlip(int index) {
    assert index >= 0 && index < numBits;
    int wordNum = index >> 6; // div 64
    int bit = index & 0x3f; // mod 64
    long bitmask = 1L << bit;
    buf.setLong(wordNum, (buf.getLong(wordNum) ^ bitmask));
  }

  /**
   * flips a bit. The index should be less than the BufBitSet size.
   */
  public void fastFlip(long index) {
    assert index >= 0 && index < numBits;
    int wordNum = (int) (index >> 6); // div 64
    int bit = (int) index & 0x3f; // mod 64
    long bitmask = 1L << bit;
    buf.setLong(wordNum, (buf.getLong(wordNum) ^ bitmask));
  }

  /** flips a bit, expanding the set size if necessary */
  public void flip(long index) {
    int wordNum = expandingWordNum(index);
    int bit = (int) index & 0x3f; // mod 64
    long bitmask = 1L << bit;
    buf.setLong(wordNum, (buf.getLong(wordNum) ^ bitmask));
  }

  /**
   * flips a bit and returns the resulting bit value. The index should be less than the BufBitSet size.
   */
  public boolean flipAndGet(int index) {
    assert index >= 0 && index < numBits;
    int wordNum = index >> 6; // div 64
    int bit = index & 0x3f; // mod 64
    long bitmask = 1L << bit;
    long longVal = buf.getLong(wordNum);
    buf.setLong(wordNum, longVal ^ bitmask);
    return (longVal & bitmask) != 0;
  }

  /**
   * flips a bit and returns the resulting bit value. The index should be less than the BufBitSet size.
   */
  public boolean flipAndGet(long index) {
    assert index >= 0 && index < numBits;
    int wordNum = (int) (index >> 6); // div 64
    int bit = (int) index & 0x3f; // mod 64
    long bitmask = 1L << bit;
    long longVal = buf.getLong(wordNum);
    buf.setLong(wordNum, longVal ^ bitmask);
    return (longVal & bitmask) != 0;
  }

  /**
   * Flips a range of bits, expanding the set size if necessary
   * 
   * @param startIndex
   *          lower index
   * @param endIndex
   *          one-past the last bit to flip
   */
  public void flip(long startIndex, long endIndex) {
    if (endIndex <= startIndex) return;
    int startWord = (int) (startIndex >> 6);

    // since endIndex is one past the end, this is index of the last
    // word to be changed.
    int endWord = expandingWordNum(endIndex - 1);

    /***
     * Grrr, java shifting wraps around so -1L>>>64 == -1 for that reason, make sure not to use endmask if the bits to
     * flip will be zero in the last word (redefine endWord to be the last changed...) long startmask = -1L <<
     * (startIndex & 0x3f); // example: 11111...111000 long endmask = -1L >>> (64-(endIndex & 0x3f)); // example:
     * 00111...111111
     ***/

    long startmask = -1L << startIndex;
    long endmask = -1L >>> -endIndex; // 64-(endIndex&0x3f) is the same as -endIndex due to wrap

    if (startWord == endWord) {
      buf.setLong(startWord, buf.getLong(startWord) ^ (startmask & endmask));
      return;
    }

    buf.setLong(startWord, buf.getLong(startWord) ^ startmask);

    for (int i = startWord + 1; i < endWord; i++) {
      buf.setLong(i, ~buf.getLong(i));
    }

    buf.setLong(endWord, buf.getLong(endWord) ^ endmask);
  }

  /*
   * public static int pop(long v0, long v1, long v2, long v3) { // derived from pop_array by setting last four elems to
   * 0. // exchanges one pop() call for 10 elementary operations // saving about 7 instructions... is there a better
   * way? long twosA=v0 & v1; long ones=v0^v1;
   * 
   * long u2=ones^v2; long twosB =(ones&v2)|(u2&v3); ones=u2^v3;
   * 
   * long fours=(twosA&twosB); long twos=twosA^twosB;
   * 
   * return (pop(fours)<<2) + (pop(twos)<<1) + pop(ones);
   * 
   * }
   */

  /** @return the number of set bits */
  public long cardinality() {
    return BitUtil.pop_array(buf, 0, wlen);
  }

  /**
   * Returns the popcount or cardinality of the intersection of the two sets. Neither set is modified.
   */
  public static long intersectionCount(BufBitSet a, BufBitSet b) {
    return BitUtil.pop_intersect(a.buf, b.buf, 0, Math.min(a.wlen, b.wlen));
  }

  /**
   * Returns the popcount or cardinality of the union of the two sets. Neither set is modified.
   */
  public static long unionCount(BufBitSet a, BufBitSet b) {
    long tot = BitUtil.pop_union(a.buf, b.buf, 0, Math.min(a.wlen, b.wlen));
    if (a.wlen < b.wlen) {
      tot += BitUtil.pop_array(b.buf, a.wlen, b.wlen - a.wlen);
    } else if (a.wlen > b.wlen) {
      tot += BitUtil.pop_array(a.buf, b.wlen, a.wlen - b.wlen);
    }
    return tot;
  }

  /**
   * Returns the popcount or cardinality of "a and not b" or "intersection(a, not(b))". Neither set is modified.
   */
  public static long andNotCount(BufBitSet a, BufBitSet b) {
    long tot = BitUtil.pop_andnot(a.buf, b.buf, 0, Math.min(a.wlen, b.wlen));
    if (a.wlen > b.wlen) {
      tot += BitUtil.pop_array(a.buf, b.wlen, a.wlen - b.wlen);
    }
    return tot;
  }

  /**
   * Returns the popcount or cardinality of the exclusive-or of the two sets. Neither set is modified.
   */
  public static long xorCount(BufBitSet a, BufBitSet b) {
    long tot = BitUtil.pop_xor(a.buf, b.buf, 0, Math.min(a.wlen, b.wlen));
    if (a.wlen < b.wlen) {
      tot += BitUtil.pop_array(b.buf, a.wlen, b.wlen - a.wlen);
    } else if (a.wlen > b.wlen) {
      tot += BitUtil.pop_array(a.buf, b.wlen, a.wlen - b.wlen);
    }
    return tot;
  }

  /**
   * Returns the index of the first set bit starting at the index specified. -1 is returned if there are no more set
   * bits.
   */
  public int nextSetBit(int index) {
    int i = index >> 6;
    if (i >= wlen) return -1;
    int subIndex = index & 0x3f; // index within the word
    long word = buf.getLong(i) >> subIndex; // skip all the bits to the right of index

    if (word != 0) {
      return (i << 6) + subIndex + Long.numberOfTrailingZeros(word);
    }

    while (++i < wlen) {
      word = buf.getLong(i);
      if (word != 0) return (i << 6) + Long.numberOfTrailingZeros(word);
    }

    return -1;
  }

  /**
   * Returns the index of the first set bit starting at the index specified. -1 is returned if there are no more set
   * bits.
   */
  public long nextSetBit(long index) {
    int i = (int) (index >>> 6);
    if (i >= wlen) return -1;
    int subIndex = (int) index & 0x3f; // index within the word
    long word = buf.getLong(i) >>> subIndex; // skip all the bits to the right of index

    if (word != 0) {
      return (((long) i) << 6) + (subIndex + Long.numberOfTrailingZeros(word));
    }

    while (++i < wlen) {
      word = buf.getLong(i);
      if (word != 0) return (((long) i) << 6) + Long.numberOfTrailingZeros(word);
    }

    return -1;
  }

  /**
   * Returns the index of the first set bit starting downwards at the index specified. -1 is returned if there are no
   * more set bits.
   */
  public int prevSetBit(int index) {
    int i = index >> 6;
    final int subIndex;
    long word;
    if (i >= wlen) {
      i = wlen - 1;
      if (i < 0) return -1;
      subIndex = 63; // last possible bit
      word = buf.getLong(i);
    } else {
      if (i < 0) return -1;
      subIndex = index & 0x3f; // index within the word
      word = (buf.getLong(i) << (63 - subIndex)); // skip all the bits to the left of index
    }

    if (word != 0) {
      return (i << 6) + subIndex - Long.numberOfLeadingZeros(word); // See LUCENE-3197
    }

    while (--i >= 0) {
      word = buf.getLong(i);
      if (word != 0) {
        return (i << 6) + 63 - Long.numberOfLeadingZeros(word);
      }
    }

    return -1;
  }

  /**
   * Returns the index of the first set bit starting downwards at the index specified. -1 is returned if there are no
   * more set bits.
   */
  public long prevSetBit(long index) {
    int i = (int) (index >> 6);
    final int subIndex;
    long word;
    if (i >= wlen) {
      i = wlen - 1;
      if (i < 0) return -1;
      subIndex = 63; // last possible bit
      word = buf.getLong(i);
    } else {
      if (i < 0) return -1;
      subIndex = (int) index & 0x3f; // index within the word
      word = (buf.getLong(i) << (63 - subIndex)); // skip all the bits to the left of index
    }

    if (word != 0) {
      return (((long) i) << 6) + subIndex - Long.numberOfLeadingZeros(word); // See LUCENE-3197
    }

    while (--i >= 0) {
      word = buf.getLong(i);
      if (word != 0) {
        return (((long) i) << 6) + 63 - Long.numberOfLeadingZeros(word);
      }
    }

    return -1;
  }

  BufBitSet cloneTest() {
    BufBitSet obs = new BufBitSet(allocator, buf.copy());
    return obs;
  }

  /** this = this AND other */
  public void intersect(BufBitSet other) {
    int newLen = Math.min(this.wlen, other.wlen);
    ByteBuf thisArr = this.buf;
    ByteBuf otherArr = other.buf;
    // testing against zero can be more efficient
    int pos = newLen;
    while (--pos >= 0) {
      thisArr.setLong(pos, thisArr.getLong(pos) & otherArr.getLong(pos));
    }
    if (this.wlen > newLen) {
      // fill zeros from the new shorter length to the old length
      fill(buf, newLen, this.wlen, 0);
    }
    this.wlen = newLen;
  }

  /** this = this OR other */
  public void union(BufBitSet other) {
    int newLen = Math.max(wlen, other.wlen);
    ensureCapacityWords(newLen);
    assert (numBits = Math.max(other.numBits, numBits)) >= 0;

    ByteBuf thisArr = this.buf;
    ByteBuf otherArr = other.buf;

    int pos = Math.min(wlen, other.wlen);
    while (--pos >= 0) {
      thisArr.setLong(pos, thisArr.getLong(pos) | otherArr.getLong(pos));
    }
    if (this.wlen < newLen) {
      System.arraycopy(otherArr, this.wlen, thisArr, this.wlen, newLen - this.wlen);
    }
    this.wlen = newLen;
  }

  /** Remove all elements set in other. this = this AND_NOT other */
  public void remove(BufBitSet other) {
    int idx = Math.min(wlen, other.wlen);
    ByteBuf thisArr = this.buf;
    ByteBuf otherArr = other.buf;
    while (--idx >= 0) {
      thisArr.setLong(idx, thisArr.getLong(idx) & ~otherArr.getLong(idx));
    }
  }

  /** this = this XOR other */
  public void xor(BufBitSet other) {
    int newLen = Math.max(wlen, other.wlen);
    ensureCapacityWords(newLen);
    assert (numBits = Math.max(other.numBits, numBits)) >= 0;

    ByteBuf thisArr = this.buf;
    ByteBuf otherArr = other.buf;
    int pos = Math.min(wlen, other.wlen);
    while (--pos >= 0) {
      thisArr.setLong(pos, thisArr.getLong(pos) ^ otherArr.getLong(pos));
    }
    if (this.wlen < newLen) {
      otherArr.readerIndex(wlen);
      otherArr.writeBytes(thisArr);
    }
    this.wlen = newLen;

  }

  // some BitSet compatability methods

  // ** see {@link intersect} */
  public void and(BufBitSet other) {
    intersect(other);
  }

  // ** see {@link union} */
  public void or(BufBitSet other) {
    union(other);
  }

  // ** see {@link andNot} */
  public void andNot(BufBitSet other) {
    remove(other);
  }

  /** returns true if the sets have any elements in common */
  public boolean intersects(BufBitSet other) {
    int pos = Math.min(this.wlen, other.wlen);
    ByteBuf thisArr = this.buf;
    ByteBuf otherArr = other.buf;
    while (--pos >= 0) {
      if ((thisArr.getLong(pos) & otherArr.getLong(pos)) != 0) return true;
    }
    return false;
  }

  public void ensureCapacityWords(int numWords) {
    if (buf.capacity() < numWords) {
      ByteBuf newBuf = allocator.buffer(numWords * 8);
      buf.writeBytes(newBuf);
      buf.release();
      buf = newBuf;
      this.numBits = numWords * 64;
    }
  }

  /**
   * Ensure that the long[] is big enough to hold numBits, expanding it if necessary. getNumWords() is unchanged by this
   * call.
   */
  public void ensureCapacity(long numBits) {
    ensureCapacityWords(bits2words(numBits));
  }

  /**
   * Lowers numWords, the number of words in use, by checking for trailing zero words.
   */
  public void trimTrailingZeros() {
    int idx = wlen - 1;
    while (idx >= 0 && buf.getLong(idx) == 0)
      idx--;
    wlen = idx + 1;
  }

  /** returns the number of 64 bit words it would take to hold numBits */
  public static int bits2words(long numBits) {
    return (int) (((numBits - 1) >>> 6) + 1);
  }

  /** returns true if both sets have the same bits set */
  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof BufBitSet)) return false;
    BufBitSet a;
    BufBitSet b = (BufBitSet) o;
    // make a the larger set.
    if (b.wlen > this.wlen) {
      a = b;
      b = this;
    } else {
      a = this;
    }

    // check for any set bits out of the range of b
    for (int i = a.wlen - 1; i >= b.wlen; i--) {
      if (a.buf.getLong(i) != 0) return false;
    }

    for (int i = b.wlen - 1; i >= 0; i--) {
      if (a.buf.getLong(i) != b.buf.getLong(i)) return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    // Start with a zero hash and use a mix that results in zero if the input is zero.
    // This effectively truncates trailing zeros without an explicit check.
    long h = 0;
    for (int i = buf.capacity(); --i >= 0;) {
      h ^= buf.getLong(i);
      h = (h << 1) | (h >>> 63); // rotate left
    }
    // fold leftmost bits into right and add a constant to prevent
    // empty sets from returning 0, which is too common.
    return (int) ((h >> 32) ^ h) + 0x98761234;
  }

  public void release() {
    this.buf.release();
  }
}
