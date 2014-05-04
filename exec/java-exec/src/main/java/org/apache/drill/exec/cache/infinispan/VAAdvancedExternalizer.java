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
package org.apache.drill.exec.cache.infinispan;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

import org.apache.drill.exec.cache.CachedVectorContainer;
import org.apache.drill.exec.cache.VectorAccessibleSerializable;
import org.apache.drill.exec.memory.BufferAllocator;
import org.infinispan.commons.marshall.AdvancedExternalizer;

import com.google.common.collect.ImmutableSet;

public class VAAdvancedExternalizer implements AdvancedExternalizer<CachedVectorContainer> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(VAAdvancedExternalizer.class);

  private BufferAllocator allocator;


  public VAAdvancedExternalizer(BufferAllocator allocator) {
    super();
    this.allocator = allocator;
  }

  static final Set<Class<? extends CachedVectorContainer>> CLASSES = //
      (Set<Class<? extends CachedVectorContainer>>) //
      (Object) ImmutableSet.of(CachedVectorContainer.class);

  @Override
  public CachedVectorContainer readObject(ObjectInput in) throws IOException, ClassNotFoundException {
    int length = in.readInt();
    byte[] b = new byte[length];
    in.read(b);
    CachedVectorContainer va = new CachedVectorContainer(b, allocator);
    return va;
  }

  @Override
  public void writeObject(ObjectOutput out, CachedVectorContainer va) throws IOException {
    out.writeInt(va.getData().length);
    out.write(va.getData());
  }

  @Override
  public Integer getId() {
    // magic number for this class, assume drill uses 3001-3100.
    return 3001;
  }

  @Override
  public Set<Class<? extends CachedVectorContainer>> getTypeClasses() {
    return CLASSES;
  }
}
