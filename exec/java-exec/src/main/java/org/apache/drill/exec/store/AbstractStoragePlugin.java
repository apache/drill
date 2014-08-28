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
import java.util.Set;

import org.apache.drill.common.JSONOptions;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.base.AbstractGroupScan;

import com.google.common.collect.ImmutableSet;

public abstract class AbstractStoragePlugin implements StoragePlugin{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AbstractStoragePlugin.class);

  protected AbstractStoragePlugin(){
  }
  
  @Override
  public boolean supportsRead() {
    return false;
  }

  @Override
  public boolean supportsWrite() {
    return false;
  }

  @Override
  public Set<StoragePluginOptimizerRule> getOptimizerRules() {
    return ImmutableSet.of();
  }

  @Override
  public AbstractGroupScan getPhysicalScan(JSONOptions selection) throws IOException {
    return getPhysicalScan(selection, AbstractGroupScan.ALL_COLUMNS);
  }
  
  @Override
  public AbstractGroupScan getPhysicalScan(JSONOptions selection, List<SchemaPath> columns) throws IOException {
    throw new UnsupportedOperationException();
  }
}
