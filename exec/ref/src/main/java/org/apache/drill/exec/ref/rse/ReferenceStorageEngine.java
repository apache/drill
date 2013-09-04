/*******************************************************************************
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
 ******************************************************************************/
package org.apache.drill.exec.ref.rse;

import java.io.IOException;
import java.util.Collection;

import org.apache.drill.common.logical.data.Scan;
import org.apache.drill.common.logical.data.Store;
import org.apache.drill.exec.ref.rops.ROP;


public interface ReferenceStorageEngine {
  public boolean supportsRead();
  public boolean supportsWrite();

  public enum PartitionCapabilities {
    NONE, HASH, SORTED;
  }

  public enum MemoryFormat {
    RECORD, FIELD;
  }

  public Collection<ReadEntry> getReadEntries(Scan scan) throws IOException;
  public RecordReader getReader(ReadEntry readEntry, ROP parentROP) throws IOException;
  public RecordRecorder getWriter(Store store) throws IOException;

  public interface ReadEntry{}
}
