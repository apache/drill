/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.io.compress.zstd;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.compress.Decompressor;

import com.github.luben.zstd.ZstdDirectBufferDecompressingStream;
import com.github.luben.zstd.util.Native;

/**
 * A {@link Decompressor} based on the zstandard compression algorithm.
 */
public class ZstdDecompressor implements Decompressor {
  private static final Log LOG = LogFactory.getLog(ZstdDecompressor.class.getName());

  private int directBufferSize;
  private ByteBuffer compressedDirectBuf = null;
  private int compressedDirectBufLen;
  private ByteBuffer uncompressedDirectBuf = null;
  private byte[] userBuf = null;
  private int userBufOff = 0, userBufLen = 0;
  private boolean finished;

  // From the zstd-jni project
  ZstdDirectBufferDecompressingStream z;

  static {
    if (!Native.isLoaded()) {
      try {
        // initIDs();
        Native.load();
      } catch (Throwable t) {
        LOG.error("failed to load ZstdDecompressor", t);
      }
    }
  }

  public static boolean isNativeCodeLoaded() {
    return Native.isLoaded();
  }

  /**
   * Creates a new compressor.
   *
   * @param directBufferSize size of the direct buffer to be used.
   */
  public ZstdDecompressor(int directBufferSize) {
    this.directBufferSize = directBufferSize;
    compressedDirectBuf = ByteBuffer.allocateDirect(directBufferSize);
    uncompressedDirectBuf = ByteBuffer.allocateDirect(directBufferSize);
    uncompressedDirectBuf.position(directBufferSize);
    z = new ZstdDirectBufferDecompressingStream(compressedDirectBuf);
  }

  /**
   * Creates a new decompressor with the default buffer size.
   */
  public ZstdDecompressor() {
    this(ZstdDirectBufferDecompressingStream.recommendedTargetBufferSize());
  }

  /**
   * Sets input data for decompression. This should be called if and only if
   * {@link #needsInput()} returns <code>true</code> indicating that more input
   * data is required. (Both native and non-native versions of various
   * Decompressors require that the data passed in via <code>b[]</code> remain
   * unmodified until the caller is explicitly notified--via
   * {@link #needsInput()}--that the buffer may be safely modified. With this
   * requirement, an extra buffer-copy can be avoided.)
   *
   * @param b   Input data
   * @param off Start offset
   * @param len Length
   */
  @Override
  public void setInput(byte[] b, int off, int len) {
    if (b == null) {
      throw new NullPointerException();
    }
    if (off < 0 || len < 0 || off > b.length - len) {
      throw new ArrayIndexOutOfBoundsException();
    }

    this.userBuf = b;
    this.userBufOff = off;
    this.userBufLen = len;

    setInputFromSavedData();

    uncompressedDirectBuf.limit(directBufferSize);
    uncompressedDirectBuf.position(directBufferSize);

    finished = false;
  }

  /**
   * If a write would exceed the capacity of the direct buffers, it is set aside
   * to be loaded by this function while the compressed data are consumed.
   */
  void setInputFromSavedData() {
    compressedDirectBufLen = Math.min(userBufLen, directBufferSize);

    // Reinitialize zstd input direct buffer
    compressedDirectBuf.rewind();
    ((ByteBuffer) compressedDirectBuf).put(userBuf, userBufOff, compressedDirectBufLen);
    compressedDirectBuf.rewind();
    compressedDirectBuf.limit(compressedDirectBufLen);
    // Note how much data is being fed to zstd
    userBufOff += compressedDirectBufLen;
    userBufLen -= compressedDirectBufLen;
  }

  /**
   * Does nothing.
   */
  @Override
  public void setDictionary(byte[] b, int off, int len) {
    // do nothing
  }

  /**
   * Returns true if the input data buffer is empty and
   * {@link #setInput(byte[], int, int)} should be called to provide more input.
   *
   * @return <code>true</code> if the input data buffer is empty and
   *         {@link #setInput(byte[], int, int)} should be called in order to
   *         provide more input.
   */
  @Override
  public boolean needsInput() {
    // Consume remaining compressed data?
    if (uncompressedDirectBuf.remaining() > 0) {
      return false;
    }

    // Check if zstd has consumed all input
    if (compressedDirectBufLen <= 0) {
      // Check if we have consumed all user-input
      if (userBufLen <= 0) {
        return true;
      } else {
        // jccote, never got into this code path.
        setInputFromSavedData();
      }
    }

    return false;
  }

  /**
   * Returns <code>false</code>.
   *
   * @return <code>false</code>.
   */
  @Override
  public boolean needsDictionary() {
    return false;
  }

  /**
   * Returns true if the end of the decompressed data output stream has been
   * reached.
   *
   * @return <code>true</code> if the end of the decompressed data output stream
   *         has been reached.
   */
  @Override
  public boolean finished() {
    return (finished && uncompressedDirectBuf.remaining() == 0);
  }

  /**
   * Fills specified buffer with uncompressed data. Returns actual number of bytes
   * of uncompressed data. A return value of 0 indicates that
   * {@link #needsInput()} should be called in order to determine if more input
   * data is required.
   *
   * @param b   Buffer for the compressed data
   * @param off Start offset of the data
   * @param len Size of the buffer
   * @return The actual number of bytes of compressed data.
   * @throws IOException
   */
  @Override
  public int decompress(byte[] b, int off, int len) throws IOException {
    if (b == null) {
      throw new NullPointerException();
    }
    if (off < 0 || len < 0 || off > b.length - len) {
      throw new ArrayIndexOutOfBoundsException();
    }

    int n = 0;

    // Check if there is uncompressed data
    n = uncompressedDirectBuf.remaining();
    if (n > 0) {
      n = Math.min(n, len);
      ((ByteBuffer) uncompressedDirectBuf).get(b, off, n);
      return n;
    }
    if (z.hasRemaining()) {
      uncompressedDirectBuf.rewind();
      uncompressedDirectBuf.limit(directBufferSize);
      n = z.read(uncompressedDirectBuf);
      uncompressedDirectBuf.rewind();
      uncompressedDirectBuf.limit(n);

      compressedDirectBufLen = compressedDirectBuf.remaining();

      if (userBufLen <= 0) {
        finished = true;
      }

      // Get atmost 'len' bytes
      n = Math.min(n, len);
      ((ByteBuffer) uncompressedDirectBuf).get(b, off, n);
    }

    return n;
  }

  /**
   * Returns <code>0</code>.
   *
   * @return <code>0</code>.
   */
  @Override
  public int getRemaining() {
    return 0;
  }

  @Override
  public void reset() {
    finished = false;
    compressedDirectBufLen = 0;
    uncompressedDirectBuf.limit(directBufferSize);
    uncompressedDirectBuf.position(directBufferSize);
    userBufOff = userBufLen = 0;
  }

  /**
   * Resets decompressor and input and output buffers so that a new set of input
   * data can be processed.
   */
  @Override
  public void end() {
    // do nothing
  }
}
