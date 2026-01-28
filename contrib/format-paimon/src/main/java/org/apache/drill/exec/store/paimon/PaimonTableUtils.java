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
package org.apache.drill.exec.store.paimon;

import com.google.common.base.Preconditions;
import org.apache.drill.exec.store.paimon.format.PaimonFormatPlugin;
import org.apache.drill.exec.store.paimon.format.PaimonFormatLocationTransformer;
import org.apache.drill.exec.store.paimon.format.PaimonFormatPluginConfig;
import org.apache.drill.exec.store.paimon.format.PaimonMetadataType;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.system.SystemTableLoader;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public final class PaimonTableUtils {
  private PaimonTableUtils() {
  }

  /**
   * Load a Paimon table directly from a filesystem path. If a metadata suffix is present,
   * returns the corresponding system table; otherwise returns the data table.
   */
  public static Table loadTable(PaimonFormatPlugin formatPlugin, String path) throws IOException {
    PaimonMetadataType metadataType = extractMetadataType(path);
    String tableLocation = metadataType == null ? path : stripMetadataType(path);
    Path tablePath = new Path(tableLocation);
    Options options = new Options();
    CatalogContext context = CatalogContext.create(options, formatPlugin.getFsConf());
    FileIO fileIO = FileIO.get(tablePath, context);
    FileStoreTable table = FileStoreTableFactory.create(fileIO, tablePath);
    // Apply time-travel and custom options at table load time.
    Map<String, String> dynamicOptions = buildDynamicOptions(formatPlugin.getConfig());
    if (!dynamicOptions.isEmpty()) {
      table = table.copy(dynamicOptions);
    }
    if (metadataType == null) {
      return table;
    }
    Table metadataTable = SystemTableLoader.load(metadataType.getName(), table);
    Preconditions.checkArgument(metadataTable != null, "Unsupported metadata table: %s", metadataType.getName());
    return metadataTable;
  }

  private static PaimonMetadataType extractMetadataType(String location) {
    int index = location.lastIndexOf(PaimonFormatLocationTransformer.METADATA_SEPARATOR);
    if (index < 0) {
      return null;
    }
    String metadataName = location.substring(index + 1);
    return PaimonMetadataType.from(metadataName);
  }

  private static String stripMetadataType(String location) {
    int index = location.lastIndexOf(PaimonFormatLocationTransformer.METADATA_SEPARATOR);
    return index < 0 ? location : location.substring(0, index);
  }

  private static Map<String, String> buildDynamicOptions(PaimonFormatPluginConfig config) {
    Map<String, String> dynamicOptions = new HashMap<>();
    if (config == null) {
      return dynamicOptions;
    }
    if (config.getProperties() != null) {
      dynamicOptions.putAll(config.getProperties());
    }

    Long snapshotId = config.getSnapshotId();
    Long snapshotAsOfTime = config.getSnapshotAsOfTime();
    Preconditions.checkArgument(snapshotId == null || snapshotAsOfTime == null,
      "Both 'snapshotId' and 'snapshotAsOfTime' cannot be specified");
    if (snapshotId != null) {
      dynamicOptions.put(CoreOptions.SCAN_MODE.key(), CoreOptions.StartupMode.FROM_SNAPSHOT.toString());
      dynamicOptions.put(CoreOptions.SCAN_SNAPSHOT_ID.key(), snapshotId.toString());
    } else if (snapshotAsOfTime != null) {
      dynamicOptions.put(CoreOptions.SCAN_MODE.key(), CoreOptions.StartupMode.FROM_TIMESTAMP.toString());
      dynamicOptions.put(CoreOptions.SCAN_TIMESTAMP_MILLIS.key(), snapshotAsOfTime.toString());
    }

    return dynamicOptions;
  }
}
