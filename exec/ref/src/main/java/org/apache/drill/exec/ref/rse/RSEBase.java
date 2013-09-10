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
package org.apache.drill.exec.ref.rse;

import java.io.IOException;
import java.util.Collection;

import org.apache.drill.common.logical.data.Scan;
import org.apache.drill.common.logical.data.Store;
import org.apache.drill.common.util.PathScanner;
import org.apache.drill.exec.ref.ExecRefConstants;
import org.apache.drill.exec.ref.RecordIterator;
import org.apache.drill.exec.ref.exceptions.MajorException;
import org.apache.drill.exec.ref.rops.ROP;

import com.typesafe.config.Config;

public abstract class RSEBase implements ReferenceStorageEngine{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RSEBase.class);
  
  @Override
  public boolean supportsRead() {
    return false;
  }

  @Override
  public boolean supportsWrite() {
    return false;
  }

  @Override
  public Collection<ReadEntry> getReadEntries(Scan scan) throws IOException {
    throw new UnsupportedOperationException(String.format("%s does not support reads.", this.getClass().getCanonicalName()));
  }

  @Override
  public RecordReader getReader(ReadEntry readEntry, ROP parentROP) throws IOException {
    throw new UnsupportedOperationException(String.format("%s does not support reads.", this.getClass().getCanonicalName()));
  }

  @Override
  public RecordRecorder getWriter(Store store) throws IOException {
    throw new UnsupportedOperationException(String.format("%s does not support writes.", this.getClass().getCanonicalName()));
  }
  
  public static Class<?>[] getSubTypes(Config config){
    Collection<Class<? extends ReferenceStorageEngine>> engines = PathScanner.scanForImplementations(ReferenceStorageEngine.class, config.getStringList(ExecRefConstants.STORAGE_ENGINE_SCAN_PACKAGES));
    return engines.toArray(new Class<?>[engines.size()]);
  }

  @SuppressWarnings("unchecked")
  protected <T extends ReadEntry> T getReadEntry(Class<T> c, ReadEntry entry){
    if(!c.isAssignableFrom(entry.getClass())) throw new MajorException(String.format("Expected entry type was invalid.  Expected entry of type %s but received type of %s.", c.getCanonicalName(), entry.getClass().getCanonicalName()));
    return (T) entry;
  }
}
