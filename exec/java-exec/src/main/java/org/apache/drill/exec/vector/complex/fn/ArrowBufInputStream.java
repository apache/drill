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
package org.apache.drill.exec.vector.complex.fn;

import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ArrowBuf;

import java.io.IOException;

import org.apache.hadoop.fs.Seekable;

/**
 * An InputStream that wraps a ArrowBuf and implements the seekable interface.
 */
public class ArrowBufInputStream extends ByteBufInputStream implements Seekable {
  //private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ArrowBufInputStream.class);

  private final ArrowBuf buffer;

  private ArrowBufInputStream(ArrowBuf buffer, int len) {
    super(buffer, len);
    this.buffer = buffer;
  }

  @Override
  public void seek(long pos) throws IOException {
    buffer.readerIndex((int) pos);
  }

  @Override
  public long getPos() throws IOException {
    return buffer.readerIndex();
  }

  @Override
  public boolean seekToNewSource(long targetPos) throws IOException {
    return false;
  }

  // Does not adopt the buffer
  public static ArrowBufInputStream getStream(int start, int end, ArrowBuf buffer) {
    ArrowBuf buf = buffer.slice(start, end - start);
    return new ArrowBufInputStream(buf, end - start);
  }
}
