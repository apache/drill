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
package org.apache.drill.exec.vector.complex.fn;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;

import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;

/**
 * A ByteArrayInputStream that supports the HDFS Seekable API.
 */
public class SeekableBAIS extends ByteArrayInputStream implements Seekable, PositionedReadable {

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SeekableBAIS.class);


  public SeekableBAIS(byte[] buf, int offset, int length) {
    super(buf, offset, length);
  }

  public SeekableBAIS(byte[] buf) {
    super(buf);
  }

  @Override
  public void seek(long pos) throws IOException {
    if(pos > buf.length){
      throw new EOFException();
    }
    this.pos = (int) pos;
    this.count = buf.length - (int) pos;
  }

  @Override
  public long getPos() throws IOException {
    return pos;
  }

  @Override
  public boolean seekToNewSource(long targetPos) throws IOException {
    return false;
  }


  /**
   * Read up to the specified number of bytes, from a given
   * position within a file, and return the number of bytes read. This does not
   * change the current offset of a file, and is thread-safe.
   *
   * <i>Warning: Not all filesystems satisfy the thread-safety requirement.</i>
   *
   * @param position position within file
   * @param buffer   destination buffer
   * @param offset   offset in the buffer
   * @param length   number of bytes to read
   * @return actual number of bytes read; -1 means "none"
   * @throws IOException IO problems.
   */
  @Override
  public int read(long position, byte[] buffer, int offset, int length) throws IOException {
    if (position >= buf.length) {
      throw new IllegalArgumentException();
    }
    if (position + length > buf.length) {
      throw new IllegalArgumentException();
    }
    if (length > buffer.length) {
      throw new IllegalArgumentException();
    }

    System.arraycopy(buf, (int) position, buffer, offset, length);
    return length;
  }

  /**
   * Read the specified number of bytes, from a given
   * position within a file. This does not
   * change the current offset of a file, and is thread-safe.
   *
   * <i>Warning: Not all filesystems satisfy the thread-safety requirement.</i>
   *
   * @param position position within file
   * @param buffer   destination buffer
   * @param offset   offset in the buffer
   * @param length   number of bytes to read
   * @throws IOException  IO problems.
   * @throws EOFException the end of the data was reached before
   *                      the read operation completed
   */
  @Override
  public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
    read(position, buffer, offset, length);
  }

  /**
   * Read number of bytes equal to the length of the buffer, from a given
   * position within a file. This does not
   * change the current offset of a file, and is thread-safe.
   *
   * <i>Warning: Not all filesystems satisfy the thread-safety requirement.</i>
   *
   * @param position position within file
   * @param buffer   destination buffer
   * @throws IOException  IO problems.
   * @throws EOFException the end of the data was reached before
   *                      the read operation completed
   */
  @Override
  public void readFully(long position, byte[] buffer) throws IOException {
    read(position, buffer, 0, buffer.length);
  }
}
