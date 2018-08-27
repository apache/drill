/*
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
package org.apache.drill.exec.work.filter;

import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
import io.netty.buffer.DrillBuf;
import org.apache.drill.exec.memory.BufferAllocator;

import java.util.Arrays;


/**
 * According to Putze et al.'s "Cache-, Hash- and Space-Efficient BloomFilter
 * Filters", see <a href="http://algo2.iti.kit.edu/singler/publications/cacheefficientbloomfilters-wea2007.pdf">this paper</a>
 * for details, the main theory is to construct tiny bucket bloom filters which benefit to
 * the cpu cache and SIMD opcode.
 */

public class BloomFilter {
  // Bytes in a bucket.
  private static final int BYTES_PER_BUCKET = 32;
  // Minimum bloom filter data size.
  private static final int MINIMUM_BLOOM_SIZE_IN_BYTES = 256;

  private DrillBuf byteBuf;

  private int numBytes;

  private int mask[] = new int[8];

  private byte[] tempBucket = new byte[32];


  public BloomFilter(int numBytes, BufferAllocator bufferAllocator) {
    int size = BloomFilter.adjustByteSize(numBytes);
    this.byteBuf = bufferAllocator.buffer(size);
    this.numBytes = byteBuf.capacity();
    this.byteBuf.writerIndex(numBytes);
  }

  public BloomFilter(int ndv, double fpp, BufferAllocator bufferAllocator) {
    this(BloomFilter.optimalNumOfBytes(ndv, fpp), bufferAllocator);
  }

  public BloomFilter(DrillBuf byteBuf) {
    this.byteBuf = byteBuf;
    this.numBytes = byteBuf.capacity();
    this.byteBuf.writerIndex(numBytes);
  }


  public static int adjustByteSize(int numBytes) {
    if (numBytes < MINIMUM_BLOOM_SIZE_IN_BYTES) {
      numBytes = MINIMUM_BLOOM_SIZE_IN_BYTES;
    }
    // 32 bytes alignment, one bucket.
    numBytes = (numBytes + 0x1F) & (~0x1F);
    return numBytes;
  }

  private void setMask(int key) {
    //8 odd numbers act as salt value to participate in the computation of the mask.
    final int SALT[] = {0x47b6137b, 0x44974d91, 0x8824ad5b, 0xa2b7289d, 0x705495c7, 0x2df1424b, 0x9efc4947, 0x5c6bfb31};

    Arrays.fill(mask, 0);

    for (int i = 0; i < 8; ++i) {
      mask[i] = key * SALT[i];
    }

    for (int i = 0; i < 8; ++i) {
      mask[i] = mask[i] >> 27;
    }

    for (int i = 0; i < 8; ++i) {
      mask[i] = 0x1 << mask[i];
    }
  }

  /**
   * Add an element's hash value to this bloom filter.
   * @param hash hash result of element.
   */
  public void insert(long hash) {
    int bucketIndex = (int) (hash >> 32) & (numBytes / BYTES_PER_BUCKET - 1);
    int key = (int) hash;
    setMask(key);
    int initialStartIndex = bucketIndex * BYTES_PER_BUCKET;
    byteBuf.getBytes(initialStartIndex, tempBucket);
    for (int i = 0; i < 8; i++) {
      //every iterate batch,we set 32 bits
      int bitsetIndex = i * 4;
      tempBucket[bitsetIndex] = (byte) (tempBucket[bitsetIndex] | (byte) (mask[i] >>> 24));
      tempBucket[bitsetIndex + 1] = (byte) (tempBucket[(bitsetIndex) + 1] | (byte) (mask[i] >>> 16));
      tempBucket[bitsetIndex + 2] = (byte) (tempBucket[(bitsetIndex) + 2] | (byte) (mask[i] >>> 8));
      tempBucket[bitsetIndex + 3] = (byte) (tempBucket[(bitsetIndex) + 3] | (byte) (mask[i]));
    }
    byteBuf.setBytes(initialStartIndex, tempBucket);
  }

  /**
   * Determine whether an element is set or not.
   *
   * @param hash the hash value of element.
   * @return false if the element is not set, true if the element is probably set.
   */
  public boolean find(long hash) {
    int bucketIndex = (int) (hash >> 32) & (numBytes / BYTES_PER_BUCKET - 1);
    int key = (int) hash;
    setMask(key);

    int startIndex = bucketIndex * BYTES_PER_BUCKET;
    byteBuf.getBytes(startIndex, tempBucket);
    for (int i = 0; i < 8; i++) {
      byte set = 0;
      int bitsetIndex = i * 4;
      set |= tempBucket[bitsetIndex] & ((byte) (mask[i] >>> 24));
      set |= tempBucket[(bitsetIndex + 1)] & ((byte) (mask[i] >>> 16));
      set |= tempBucket[(bitsetIndex + 2)] & ((byte) (mask[i] >>> 8));
      set |= tempBucket[(bitsetIndex + 3)] & ((byte) mask[i]);
      if (0 == set) {
        return false;
      }
    }
    return true;
  }

  /**
   * Merge this bloom filter with other one
   * @param other
   */
  public void or(BloomFilter other) {
    int otherLength = other.byteBuf.capacity();
    int thisLength = this.byteBuf.capacity();
    Preconditions.checkArgument(otherLength == thisLength);
    Preconditions.checkState(otherLength % BYTES_PER_BUCKET == 0);
    Preconditions.checkState(thisLength % BYTES_PER_BUCKET == 0);
    byte[] otherTmpBucket = new byte[BYTES_PER_BUCKET];
    for (int i = 0; i < thisLength / BYTES_PER_BUCKET; i++) {
      byteBuf.getBytes(i * BYTES_PER_BUCKET, tempBucket);
      other.byteBuf.getBytes(i * BYTES_PER_BUCKET, otherTmpBucket);
      for (int j = 0; j < BYTES_PER_BUCKET; j++) {
        tempBucket[j] = (byte) (tempBucket[j] | otherTmpBucket[j]);
      }
      this.byteBuf.setBytes(i, tempBucket);
    }
  }

  /**
   * Calculate optimal size according to the number of distinct values and false positive probability.
   * See http://en.wikipedia.org/wiki/Bloom_filter#Probability_of_false_positives for the formula.
   * @param ndv: The number of distinct values.
   * @param fpp: The false positive probability.
   * @return optimal number of bytes of given ndv and fpp.
   */
  public static int optimalNumOfBytes(long ndv, double fpp) {
    int bits = (int) (-ndv * Math.log(fpp) / (Math.log(2) * Math.log(2)));
    bits--;
    bits |= bits >> 1;
    bits |= bits >> 2;
    bits |= bits >> 4;
    bits |= bits >> 8;
    bits |= bits >> 16;
    bits++;
    int bytes = bits/8;
    return bytes;
  }

  public DrillBuf getContent() {
    return byteBuf;
  }
}
