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
package org.apache.drill.exec.store.parquet;

import org.apache.drill.metastore.ParquetTableMetadataProvider;
import org.apache.drill.metastore.metadata.TableMetadataProviderBuilder;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.store.dfs.FileSelection;
import org.apache.drill.exec.store.dfs.ReadEntryWithPath;
import org.apache.hadoop.fs.Path;

import java.util.List;

/**
 * Builder for {@link ParquetTableMetadataProvider}.
 */
public interface ParquetFileTableMetadataProviderBuilder extends TableMetadataProviderBuilder {

  ParquetFileTableMetadataProviderBuilder withEntries(List<ReadEntryWithPath> entries);

  ParquetFileTableMetadataProviderBuilder withSelectionRoot(Path selectionRoot);

  ParquetFileTableMetadataProviderBuilder withCacheFileRoot(Path cacheFileRoot);

  ParquetFileTableMetadataProviderBuilder withReaderConfig(ParquetReaderConfig readerConfig);

  ParquetFileTableMetadataProviderBuilder withFileSystem(DrillFileSystem fs);

  ParquetFileTableMetadataProviderBuilder withCorrectCorruptedDates(boolean autoCorrectCorruptedDates);

  ParquetFileTableMetadataProviderBuilder withSelection(FileSelection selection);
}
