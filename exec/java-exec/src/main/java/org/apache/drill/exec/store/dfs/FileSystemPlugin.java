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
package org.apache.drill.exec.store.dfs;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import net.hydromatic.optiq.Schema;
import net.hydromatic.optiq.SchemaPlus;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.common.logical.data.Scan;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.AbstractStoragePlugin;
import org.apache.drill.exec.store.ClassPathFileSystem;
import org.apache.drill.exec.store.dfs.shim.DrillFileSystem;
import org.apache.drill.exec.store.dfs.shim.FileSystemCreator;
import org.apache.hadoop.conf.Configuration;

import com.beust.jcommander.internal.Lists;
import com.beust.jcommander.internal.Maps;

/**
 * A Storage engine associated with a Hadoop FileSystem Implementation. Examples include HDFS, MapRFS, QuantacastFileSystem,
 * LocalFileSystem, as well Apache Drill specific CachedFileSystem, ClassPathFileSystem and LocalSyncableFileSystem.
 * Tables are file names, directories and path patterns. This storage engine delegates to FSFormatEngines but shares
 * references to the FileSystem configuration and path management.
 */
public class FileSystemPlugin extends AbstractStoragePlugin{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FileSystemPlugin.class);

  private final FileSystemSchemaFactory schemaFactory;
  private Map<String, FormatPlugin> formatsByName;
  private Map<FormatPluginConfig, FormatPlugin> formatPluginsByConfig;
  private FileSystemConfig config;
  private DrillbitContext context;
  private final DrillFileSystem fs;
  
  public FileSystemPlugin(FileSystemConfig config, DrillbitContext context, String name) throws ExecutionSetupException{
    try{
      this.config = config;
      this.context = context;
      
      Configuration fsConf = new Configuration();
      fsConf.set("fs.default.name", config.connection);
      fsConf.set("fs.classpath.impl", ClassPathFileSystem.class.getName());
      this.fs = FileSystemCreator.getFileSystem(context.getConfig(), fsConf);
      this.formatsByName = FormatCreator.getFormatPlugins(context, fs, config);
      List<FormatMatcher> matchers = Lists.newArrayList();
      formatPluginsByConfig = Maps.newHashMap();
      for(FormatPlugin p : formatsByName.values()){
        matchers.add(p.getMatcher());
        formatPluginsByConfig.put(p.getConfig(), p);
      }
      
      List<WorkspaceSchemaFactory> factories = null;
      if(config.workspaces == null || config.workspaces.isEmpty()){
        factories = Collections.singletonList(new WorkspaceSchemaFactory("default", name, fs, "/", matchers));
      }else{
        factories = Lists.newArrayList();
        for(Map.Entry<String, String> space : config.workspaces.entrySet()){
          factories.add(new WorkspaceSchemaFactory(space.getKey(), name, fs, space.getValue(), matchers));
        }
      }
      this.schemaFactory = new FileSystemSchemaFactory(name, factories);
    }catch(IOException e){
      throw new ExecutionSetupException("Failure setting up file system plugin.", e);
    }
  }
  
  @Override
  public boolean supportsRead() {
    return true;
  }
  
  @Override
  public AbstractGroupScan getPhysicalScan(Scan scan) throws IOException {
    FormatSelection formatSelection = scan.getSelection().getWith(context.getConfig(), FormatSelection.class);
    FormatPlugin plugin;
    if(formatSelection.getFormat() instanceof NamedFormatPluginConfig){
      plugin = formatsByName.get( ((NamedFormatPluginConfig) formatSelection.getFormat()).name);
    }else{
      plugin = formatPluginsByConfig.get(formatSelection.getFormat());
    }
    if(plugin == null) throw new IOException(String.format("Failure getting requested format plugin named '%s'.  It was not one of the format plugins registered.", formatSelection.getFormat()));
    return plugin.getGroupScan(scan.getOutputReference(), formatSelection.getSelection());
  }
  
  @Override
  public Schema createAndAddSchema(SchemaPlus parent) {
    return schemaFactory.add(parent);
  }
  
  public FormatPlugin getFormatPlugin(String name){
    return formatsByName.get(name);
  }
  
  public FormatPlugin getFormatPlugin(FormatPluginConfig config){
    if(config instanceof NamedFormatPluginConfig){
      return formatsByName.get(((NamedFormatPluginConfig) config).name);
    }else{
      return formatPluginsByConfig.get(config);
    }
  }

}
