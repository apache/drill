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
package org.apache.drill.exec.cache;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;
import org.apache.drill.common.util.DataInputInputStream;
import org.apache.drill.common.util.DataOutputOutputStream;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.server.DrillbitContext;

import java.io.*;

/**
 * Wraps a DrillSerializable object. Objects of this class can be put in the HazelCast implementation of Distributed Cache
 */
public class HCVectorAccessibleSerializer implements StreamSerializer<VectorAccessibleSerializable> {

  private BufferAllocator allocator;

  public HCVectorAccessibleSerializer(BufferAllocator allocator) {
    this.allocator = allocator;
  }

  public VectorAccessibleSerializable read(ObjectDataInput in) throws IOException {
    VectorAccessibleSerializable va = new VectorAccessibleSerializable(allocator);
    va.readFromStream(DataInputInputStream.constructInputStream(in));
    return va;
  }

  public void write(ObjectDataOutput out, VectorAccessibleSerializable va) throws IOException {
    va.writeToStream(DataOutputOutputStream.constructOutputStream(out));
  }

  public void destroy() {}

  public int getTypeId() {
    return 1;
  }
}
