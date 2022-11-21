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
package org.apache.drill.exec.store.delta;

import org.apache.drill.exec.metastore.MetadataProviderManager;
import org.apache.drill.exec.store.delta.format.DeltaFormatPlugin;
import org.apache.drill.exec.store.dfs.ReadEntryWithPath;
import org.apache.drill.exec.store.parquet.BaseParquetMetadataProvider;
import org.apache.drill.exec.store.parquet.metadata.Metadata;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * This is Metadata provider for Delta tables, which are read by Drill native Parquet reader
 */
public class DeltaParquetTableMetadataProvider extends BaseParquetMetadataProvider {

  private final DeltaFormatPlugin deltaFormatPlugin;

  private DeltaParquetTableMetadataProvider(Builder builder) throws IOException {
    super(builder);

    this.deltaFormatPlugin = builder.formatPlugin;

    init((BaseParquetMetadataProvider) builder.metadataProviderManager().getTableMetadataProvider());
  }

  @Override
  protected void initInternal() throws IOException {
    Map<FileStatus, FileSystem> fileStatusConfMap = new LinkedHashMap<>();
    for (ReadEntryWithPath entry : entries) {
      Path path = entry.getPath();
      FileSystem fs = path.getFileSystem(deltaFormatPlugin.getFsConf());
      fileStatusConfMap.put(fs.getFileStatus(Path.getPathWithoutSchemeAndAuthority(path)), fs);
    }
    parquetTableMetadata = Metadata.getParquetTableMetadata(fileStatusConfMap, readerConfig);
  }

  public static class Builder extends BaseParquetMetadataProvider.Builder<Builder> {
    private DeltaFormatPlugin formatPlugin;

    public Builder(MetadataProviderManager source) {
      super(source);
    }

    protected Builder withFormatPlugin(DeltaFormatPlugin formatPlugin) {
      this.formatPlugin = formatPlugin;
      return self();
    }

    @Override
    protected Builder self() {
      return this;
    }

    @Override
    public DeltaParquetTableMetadataProvider build() throws IOException {
      return new DeltaParquetTableMetadataProvider(this);
    }
  }
}
