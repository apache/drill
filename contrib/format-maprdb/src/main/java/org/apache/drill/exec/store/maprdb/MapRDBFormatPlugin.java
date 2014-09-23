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
package org.apache.drill.exec.store.maprdb;

import com.google.common.collect.ImmutableSet;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.physical.base.AbstractWriter;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.StoragePluginOptimizerRule;
import org.apache.drill.exec.store.dfs.FileSelection;
import org.apache.drill.exec.store.dfs.FileSystemPlugin;
import org.apache.drill.exec.store.dfs.FormatMatcher;
import org.apache.drill.exec.store.dfs.FormatPlugin;
import org.apache.drill.exec.store.dfs.shim.DrillFileSystem;
import org.apache.drill.exec.store.hbase.HBaseScanSpec;

import java.io.IOException;
import java.util.List;
import java.util.Set;

public class MapRDBFormatPlugin implements FormatPlugin{

    private final StoragePluginConfig storageConfig;
    private final MapRDBFormatPluginConfig config;
    private final MapRDBFormatMatcher matcher;
    private final DrillFileSystem fs;
    private final DrillbitContext context;
    private final String name;

    public MapRDBFormatPlugin(String name, DrillbitContext context, DrillFileSystem fs, StoragePluginConfig storageConfig){
        this(name, context, fs, storageConfig, new MapRDBFormatPluginConfig());
    }

    public MapRDBFormatPlugin(String name, DrillbitContext context, DrillFileSystem fs, StoragePluginConfig storageConfig, MapRDBFormatPluginConfig formatConfig){
        this.context = context;
        this.config = formatConfig;
        this.matcher = new MapRDBFormatMatcher(this, fs);
        this.storageConfig = storageConfig;
        this.fs = fs;
        this.name = name == null ? "maprdb" : name;
    }
	@Override
	public boolean supportsRead() {
		return true;
	}

	@Override
	public boolean supportsWrite() {
		return false;
	}

	@Override
	public FormatMatcher getMatcher() {
    return matcher;
	}

	@Override
	public AbstractWriter getWriter(PhysicalOperator child, String location)
			throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public AbstractGroupScan getGroupScan(FileSelection selection)
            throws IOException {
        return getGroupScan(selection, null);
	}

	@Override
	public Set<StoragePluginOptimizerRule> getOptimizerRules() {
        return ImmutableSet.of();
	}

	@Override
	public AbstractGroupScan getGroupScan(FileSelection selection,
			List<SchemaPath> columns) throws IOException {
        List<String> files = selection.getAsFiles();
        assert(files.size() == 1);
        String tableName = files.get(0);
        HBaseScanSpec scanSpec = new HBaseScanSpec(tableName);
        try {
            return new MapRDBGroupScan((FileSystemPlugin)(context.getStorage().getPlugin(storageConfig)), this, scanSpec, columns);
        } catch (ExecutionSetupException e) {
            e.printStackTrace();
            return null;
        }
    }

	@Override
	public FormatPluginConfig getConfig() {
		return config;
	}

	@Override
	public StoragePluginConfig getStorageConfig() {
		return storageConfig;
	}

	@Override
	public DrillFileSystem getFileSystem() {
		return fs;
	}

	@Override
	public DrillbitContext getContext() {
		return context;
	}

	@Override
	public String getName() {
		return name;
	}
	
}
