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
package org.apache.drill.exec.cache;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.record.VectorAccessible;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.WritableBatch;
import org.apache.drill.exec.record.selection.SelectionVector2;

/**
 * Serializes vector containers to an output stream or from
 * an input stream.
 */

public class VectorSerializer {

  /**
   * Writes multiple VectorAccessible or VectorContainer
   * objects to an output stream.
   */

  public static class Writer {

    private final OutputStream stream;
    private final BufferAllocator allocator;
    private boolean retain;
    private long timeNs;

    public Writer(BufferAllocator allocator, OutputStream stream) {
      this.allocator = allocator;
      this.stream = stream;
    }

    public Writer retain() {
      retain = true;
      return this;
    }

    public Writer write(VectorAccessible va) throws IOException {
      return write(va, null);
    }

    @SuppressWarnings("resource")
    public Writer write(VectorAccessible va, SelectionVector2 sv2) throws IOException {
      WritableBatch batch = WritableBatch.getBatchNoHVWrap(
          va.getRecordCount(), va, sv2 != null);
      return write(batch, sv2);
    }

    public Writer write(WritableBatch batch, SelectionVector2 sv2) throws IOException {
      VectorAccessibleSerializable vas;
      if (sv2 == null) {
        vas = new VectorAccessibleSerializable(batch, allocator);
      } else {
        vas = new VectorAccessibleSerializable(batch, sv2, allocator);
      }
      if (retain) {
        vas.writeToStreamAndRetain(stream);
      } else {
        vas.writeToStream(stream);
      }
      timeNs += vas.getTimeNs();
      return this;
    }

    public long timeNs() { return timeNs; }
  }

  /**
   * Read one or more vector containers from an input stream.
   */

  public static class Reader {
    private final InputStream stream;
    private long timeNs;
    private final VectorAccessibleSerializable vas;

    public Reader(BufferAllocator allocator, InputStream stream) {
      this.stream = stream;
      vas = new VectorAccessibleSerializable(allocator);
    }

    public VectorContainer read() throws IOException {
      vas.readFromStream(stream);
      timeNs = vas.getTimeNs();
      return vas.get();
    }

    public SelectionVector2 sv2() { return vas.getSv2(); }

    public long timeNs() { return timeNs; }
  }

  public static Writer writer(BufferAllocator allocator, OutputStream stream) {
    return new Writer(allocator, stream);
  }

  public static Reader reader(BufferAllocator allocator, InputStream stream) {
    return new Reader(allocator, stream);
  }
}
