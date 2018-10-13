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

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.Decompressor;

import org.apache.drill.shaded.guava.com.google.common.base.Stopwatch;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public class ZstdCompressionInputStream extends CompressionInputStream {
  protected Decompressor decompressor = null;
  protected byte[] buffer;
  protected boolean eof = false;
  protected boolean closed = false;

  public ZstdCompressionInputStream(InputStream in, Decompressor decompressor, int bufferSize) throws IOException {
    super(in);

    if (decompressor == null) {
      throw new NullPointerException();
    } else if (bufferSize <= 0) {
      throw new IllegalArgumentException("Illegal bufferSize");
    }

    this.decompressor = decompressor;
    buffer = new byte[bufferSize];
  }

  public ZstdCompressionInputStream(InputStream in, Decompressor decompressor) throws IOException {
    this(in, decompressor, 1 * 1024);
  }

  /**
   * Allow derived classes to directly set the underlying stream.
   *
   * @param in
   *          Underlying input stream.
   * @throws IOException
   */
  protected ZstdCompressionInputStream(InputStream in) throws IOException {
    super(in);
  }

  private byte[] oneByte = new byte[1];

  @Override
  public int read() throws IOException {
    checkStream();
    return (read(oneByte, 0, oneByte.length) == -1) ? -1 : (oneByte[0] & 0xff);
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    checkStream();

    if ((off | len | (off + len) | (b.length - (off + len))) < 0) {
      throw new IndexOutOfBoundsException();
    } else if (len == 0) {
      return 0;
    }

    return decompress(b, off, len);
  }

  protected int decompress(byte[] uncompressed, int off, int len) throws IOException {
    if (decompressor.needsInput()) {
      int c = getCompressedData();
      if (c < 0) {
        return -1;
      }
      decompressor.setInput(buffer, 0, c);
    }

    return decompressor.decompress(uncompressed, 0, len);
  }

  protected int getCompressedData() throws IOException {
    checkStream();

    return in.read(buffer, 0, buffer.length);
  }

  protected void checkStream() throws IOException {
    if (closed) {
      throw new IOException("Stream closed");
    }
  }

  @Override
  public void resetState() throws IOException {
    decompressor.reset();
  }

  private byte[] skipBytes = new byte[512];

  @Override
  public long skip(long n) throws IOException {
    // Sanity checks
    if (n < 0) {
      throw new IllegalArgumentException("negative skip length");
    }
    checkStream();

    // Read 'n' bytes
    int skipped = 0;
    while (skipped < n) {
      int len = Math.min(((int) n - skipped), skipBytes.length);
      len = read(skipBytes, 0, len);
      if (len == -1) {
        eof = true;
        break;
      }
      skipped += len;
    }
    return skipped;
  }

  @Override
  public int available() throws IOException {
    checkStream();
    return (eof) ? 0 : 1;
  }

  @Override
  public void close() throws IOException {
    if (!closed) {
      in.close();
      closed = true;
    }
  }

  @Override
  public boolean markSupported() {
    return false;
  }

  @Override
  public synchronized void mark(int readlimit) {
  }

  @Override
  public synchronized void reset() throws IOException {
    throw new IOException("mark/reset not supported");
  }
}
