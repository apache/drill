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
package org.apache.drill.exec.store;

import java.io.IOException;
import java.util.List;

import org.apache.drill.common.JSONOptions;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.metastore.MetadataProviderManager;

import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.server.options.SessionOptionManager;
import org.apache.drill.exec.store.dfs.FormatPlugin;

/**
 * Abstract class for StorePlugin implementations.
 * See StoragePlugin for description of the interface intent and its methods.
 */
public abstract class AbstractStoragePlugin implements StoragePlugin {

  protected final DrillbitContext context;
  private final String name;

  protected AbstractStoragePlugin(DrillbitContext inContext, String inName) {
    this.context = inContext;
    this.name = inName == null ? null : inName.toLowerCase();
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
  public boolean supportsInsert() {
    return false;
  }

  @Override
  public AbstractGroupScan getPhysicalScan(String userName, JSONOptions selection, SessionOptionManager options) throws IOException {
    return getPhysicalScan(userName, selection);
  }

  @Override
  public AbstractGroupScan getPhysicalScan(String userName, JSONOptions selection, SessionOptionManager options, MetadataProviderManager metadataProviderManager) throws IOException {
    return getPhysicalScan(userName, selection, options);
  }

  @Override
  public AbstractGroupScan getPhysicalScan(String userName, JSONOptions selection) throws IOException {
    return getPhysicalScan(userName, selection, AbstractGroupScan.ALL_COLUMNS);
  }

  @Override
  public AbstractGroupScan getPhysicalScan(String userName, JSONOptions selection, List<SchemaPath> columns, SessionOptionManager options) throws IOException {
    return getPhysicalScan(userName, selection, columns);
  }

  @Override
  public AbstractGroupScan getPhysicalScan(String userName, JSONOptions selection, List<SchemaPath> columns, SessionOptionManager options, MetadataProviderManager metadataProviderManager) throws IOException {
    return getPhysicalScan(userName, selection, columns, options);
  }

  @Override
  public AbstractGroupScan getPhysicalScan(String userName, JSONOptions selection, List<SchemaPath> columns) throws IOException {
    throw new UnsupportedOperationException("Physical scan is not supported by '" + getName() + "' storage plugin.");
  }

  @Override
  public void start() throws IOException { }

  @Override
  public void close() throws Exception { }

  @Override
  public FormatPlugin getFormatPlugin(FormatPluginConfig config) {
    throw new UnsupportedOperationException(String.format("%s doesn't support format plugins", getClass().getName()));
  }

  @Override
  public String getName() {
    return name;
  }

  public DrillbitContext getContext() {
    return context;
  }

  @Override
  public String toString() {
    return name;
  }
}
