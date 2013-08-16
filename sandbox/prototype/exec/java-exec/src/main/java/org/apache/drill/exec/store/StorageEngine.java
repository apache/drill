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
package org.apache.drill.exec.store;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import org.apache.drill.common.logical.StorageEngineConfig;
import org.apache.drill.common.logical.data.Scan;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.ReadEntry;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;

import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimap;

public interface StorageEngine {
  public boolean supportsRead();

  public boolean supportsWrite();

  public enum PartitionCapabilities {
    NONE, HASH, RANGE;
  }

  public List<QueryOptimizerRule> getOptimizerRules();

  /**
   * Get the physical scan operator populated with a set of read entries required for the particular GroupScan (read) node.
   * This is somewhat analogous to traditional MapReduce. The difference is, this is the most granular split paradigm.
   * 
   * @param scan
   *          The configured scan entries.
   * @return
   * @throws IOException
   */
  public AbstractGroupScan getPhysicalScan(Scan scan) throws IOException;

  public SchemaProvider getSchemaProvider();
  
  
  /**
   * Get the set of Drillbit endpoints that are available for each read entry. Note that it is possible for a read entry
   * to have no Drillbit locations. In that case, the multimap will contain no values for that read entry.
   * 
   * @return Multimap of ReadEntry > List<DrillbitEndpoint> for ReadEntries with available locations.
   */
  public ListMultimap<ReadEntry, DrillbitEndpoint> getReadLocations(Collection<ReadEntry> entries);

  /**
   * Apply read entry assignments based on the list of actually assigned Endpoints. A storage engine is allowed to
   * update or modify the read entries based on the nature of the assignments. For example, if two blocks are initially
   * considered separate read entries but then the storage engine realizes that the assignments for those two reads are
   * on the same system, the storage engine may decide to collapse those entries into a single read entry that covers
   * both original read entries.
   * 
   * @param assignments
   * @param entries
   * @return
   */
  public Multimap<DrillbitEndpoint, ReadEntry> getEntryAssignments(List<DrillbitEndpoint> assignments,
      Collection<ReadEntry> entries);

  /**
   * Get a particular reader for a fragment context.
   * 
   * @param context
   * @param readEntry
   * @return
   * @throws IOException
   */
  public RecordReader getReader(FragmentContext context, ReadEntry readEntry) throws IOException;

  /**
   * Apply write entry assignments based on the list of actually assigned endpoints. A storage engine is allowed to
   * rewrite the WriteEntries if desired based on the nature of the assignments. For example, a storage engine could
   * hold off actually determining the specific level of partitioning required until it finds out exactly the number of
   * nodes associated with the operation.
   * 
   * @param assignments
   * @param entries
   * @return
   */
  public Multimap<DrillbitEndpoint, WriteEntry> getWriteAssignments(List<DrillbitEndpoint> assignments,
      Collection<ReadEntry> entries);

  /**
   * 
   * @param context
   * @param writeEntry
   * @return
   * @throws IOException
   */
  public RecordRecorder getWriter(FragmentContext context, WriteEntry writeEntry) throws IOException;

  public interface WriteEntry {
  }

  public static class Cost {
    public long disk;
    public long network;
    public long memory;
    public long cpu;
  }
}
