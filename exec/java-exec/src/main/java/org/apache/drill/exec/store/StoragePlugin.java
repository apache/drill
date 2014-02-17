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
package org.apache.drill.exec.store;

import java.io.IOException;
import java.util.List;

import net.hydromatic.optiq.Schema;
import net.hydromatic.optiq.SchemaPlus;

import org.apache.drill.common.logical.data.Scan;
import org.apache.drill.exec.physical.base.AbstractGroupScan;

public interface StoragePlugin {
  public boolean supportsRead();

  public boolean supportsWrite();

  public List<QueryOptimizerRule> getOptimizerRules();

  /**
   * Get the physical scan operator for the particular GroupScan (read) node.
   * 
   * @param scan
   *          The configured scan with a storage engine specific selection.
   * @return
   * @throws IOException
   */
  public AbstractGroupScan getPhysicalScan(Scan scan) throws IOException;
  
  public Schema createAndAddSchema(SchemaPlus parent);

}
