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

import io.netty.buffer.DrillBuf;

import java.nio.ByteBuffer;
import java.util.Random;

import org.apache.drill.common.DeferredException;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.ExecTest;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.memory.RootAllocatorFactory;
import org.apache.drill.exec.store.parquet.DirectCodecFactory;
import org.apache.drill.exec.store.parquet.DirectCodecFactory.ByteBufBytesInput;
import org.apache.drill.exec.store.parquet.DirectCodecFactory.DirectBytesDecompressor;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

import parquet.bytes.BytesInput;
import parquet.hadoop.CodecFactory.BytesCompressor;
import parquet.hadoop.metadata.CompressionCodecName;

public class TestDirectCodecFactory extends ExecTest {
  //private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestDirectCodecFactory.class);
  private final DrillConfig drillConfig = DrillConfig.create();

  private static enum Decompression {
    ON_HEAP, OFF_HEAP, DRILLBUF
  }

  private void test(int size, CompressionCodecName codec, boolean useOnHeapCompression, Decompression decomp) throws Exception {
    DrillBuf rawBuf = null;
    DrillBuf outBuf = null;
    try (final BufferAllocator allocator = RootAllocatorFactory.newRoot(drillConfig);
        final DirectCodecFactory codecFactory = new DirectCodecFactory(new Configuration(), allocator)) {
      try {
        rawBuf = allocator.buffer(size);
        final byte[] rawArr = new byte[size];
        outBuf = allocator.buffer(size * 2);
        final Random r = new Random();
        final byte[] random = new byte[1024];
        int pos = 0;
        while (pos < size) {
          r.nextBytes(random);
          rawBuf.writeBytes(random);
          System.arraycopy(random, 0, rawArr, pos, random.length);
          pos += random.length;
        }

        final BytesCompressor c = codecFactory.getCompressor(codec, 64 * 1024);
        final DirectBytesDecompressor d = codecFactory.getDecompressor(codec);

        final BytesInput compressed;
        if (useOnHeapCompression) {
          compressed = c.compress(BytesInput.from(rawArr));
        } else {
          compressed = c.compress(new ByteBufBytesInput(rawBuf));
        }

        switch (decomp) {
        case DRILLBUF: {
          final ByteBuffer buf = compressed.toByteBuffer();
          final DrillBuf b = allocator.buffer(buf.capacity());
          try {
            b.writeBytes(buf);
            d.decompress(b, (int) compressed.size(), outBuf, size);
            for (int i = 0; i < size; i++) {
              Assert.assertTrue("Data didn't match at " + i, outBuf.getByte(i) == rawBuf.getByte(i));
            }
          } finally {
            b.release();
          }
          break;
        }

        case OFF_HEAP: {
          final ByteBuffer buf = compressed.toByteBuffer();
          final DrillBuf b = allocator.buffer(buf.capacity());
          try {
            b.writeBytes(buf);
            final BytesInput input = d.decompress(new ByteBufBytesInput(b), size);
            Assert.assertArrayEquals(input.toByteArray(), rawArr);
          } finally {
            b.release();
          }
          break;
        }
        case ON_HEAP: {
          final byte[] buf = compressed.toByteArray();
          final BytesInput input = d.decompress(BytesInput.from(buf), size);
          Assert.assertArrayEquals(input.toByteArray(), rawArr);
          break;
        }
        }
      } catch (Exception e) {
        final String msg = String.format(
            "Failure while testing Codec: %s, OnHeapCompressionInput: %s, Decompression Mode: %s, Data Size: %d",
            codec.name(),
            useOnHeapCompression, decomp.name(), size);
        System.out.println(msg);
        throw new RuntimeException(msg, e);
      } finally {
        if (rawBuf != null) {
          rawBuf.release();
        }
        if (outBuf != null) {
          outBuf.release();
        }
      }
    }
  }

  @Test
  public void compressionCodecs() throws Exception {
    final int[] sizes = { 4 * 1024, 1 * 1024 * 1024 };
    final boolean[] comp = { true, false };

    try (final DeferredException ex = new DeferredException()) {
      for (final int size : sizes) {
        for (final boolean useOnHeapComp : comp) {
          for (final Decompression decomp : Decompression.values()) {
            for (final CompressionCodecName codec : CompressionCodecName.values()) {
              if (codec == CompressionCodecName.LZO) {
                // not installed as gpl.
                continue;
              }
              try {
                test(size, codec, useOnHeapComp, decomp);
              } catch (Exception e) {
                ex.addException(e);
              }
            }
          }
        }
      }
    }
  }
}
