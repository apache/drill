/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store.dfs;

import org.apache.drill.exec.ops.OperatorStats;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.ReadOption;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.ByteBufferPool;

import java.io.FileDescriptor;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.EnumSet;


/**
 * Wrapper around FSDataInputStream to collect IO Stats.
 */
public class DrillFSDataInputStream extends FSDataInputStream {
  private FSDataInputStream underlyingIs;

  public DrillFSDataInputStream(FSDataInputStream in, OperatorStats operatorStats) throws IOException {
    super(new WrappedInputStream(in, operatorStats));
    this.underlyingIs = in;
  }

  @Override
  public synchronized void seek(long desired) throws IOException {
    underlyingIs.seek(desired);
  }

  @Override
  public long getPos() throws IOException {
    return underlyingIs.getPos();
  }

  @Override
  public int read(long position, byte[] buffer, int offset, int length) throws IOException {
    return underlyingIs.read(position, buffer, offset, length);
  }

  @Override
  public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
    underlyingIs.readFully(position, buffer, offset, length);
  }

  @Override
  public void readFully(long position, byte[] buffer) throws IOException {
    underlyingIs.readFully(position, buffer);
  }

  @Override
  public boolean seekToNewSource(long targetPos) throws IOException {
    return underlyingIs.seekToNewSource(targetPos);
  }

  @Override
  public InputStream getWrappedStream() {
    return underlyingIs.getWrappedStream();
  }

  @Override
  public int read(ByteBuffer buf) throws IOException {
    return underlyingIs.read(buf);
  }

  @Override
  public FileDescriptor getFileDescriptor() throws IOException {
    return underlyingIs.getFileDescriptor();
  }

  @Override
  public void setReadahead(Long readahead) throws IOException, UnsupportedOperationException {
    underlyingIs.setReadahead(readahead);
  }

  @Override
  public void setDropBehind(Boolean dropBehind) throws IOException, UnsupportedOperationException {
    underlyingIs.setDropBehind(dropBehind);
  }

  @Override
  public ByteBuffer read(ByteBufferPool bufferPool, int maxLength, EnumSet<ReadOption> opts) throws IOException, UnsupportedOperationException {
    return underlyingIs.read(bufferPool, maxLength, opts);
  }

  @Override
  public void releaseBuffer(ByteBuffer buffer) {
    underlyingIs.releaseBuffer(buffer);
  }

  /**
   * We need to wrap the FSDataInputStream inside a InputStream, because read() method in InputStream is
   * overridden in FilterInputStream (super class of FSDataInputStream) as final, so we can not override in
   * DrillFSDataInputStream.
   */
  private static class WrappedInputStream extends InputStream implements Seekable, PositionedReadable {
    final FSDataInputStream is;
    final OperatorStats operatorStats;

    WrappedInputStream(FSDataInputStream is, OperatorStats operatorStats) {
      this.is = is;
      this.operatorStats = operatorStats;
    }

    /**
     * Most of the read are going to be block reads which use {@link #read(byte[], int,
     * int)}. So not adding stats for single byte reads.
     */
    @Override
    public int read() throws IOException {
      return is.read();
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      operatorStats.startWait();
      int numBytesRead;
      try {
        numBytesRead = is.read(b, off, len);
      } finally {
        operatorStats.stopWait();
      }

      return numBytesRead;
    }

    @Override
    public int read(long position, byte[] buffer, int offset, int length) throws IOException {
      return is.read(position, buffer, offset, length);
    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
      is.readFully(position, buffer, offset, length);
    }

    @Override
    public void readFully(long position, byte[] buffer) throws IOException {
      is.readFully(position, buffer);
    }

    @Override
    public void seek(long pos) throws IOException {
      is.seek(pos);
    }

    @Override
    public long getPos() throws IOException {
      return is.getPos();
    }

    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
      return is.seekToNewSource(targetPos);
    }
  }
}
