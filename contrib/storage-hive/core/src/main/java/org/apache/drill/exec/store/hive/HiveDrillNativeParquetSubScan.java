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
package org.apache.drill.exec.store.hive;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.store.StoragePluginRegistry;

import java.io.IOException;
import java.util.List;

/**
 * Extension of {@link HiveSubScan} which support reading Hive tables using Drill's native parquet reader.
 */
@JsonTypeName("hive-drill-native-parquet-sub-scan")
public class HiveDrillNativeParquetSubScan extends HiveSubScan {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HiveDrillNativeParquetSubScan.class);

  @JsonCreator
  public HiveDrillNativeParquetSubScan(@JacksonInject StoragePluginRegistry registry,
                                       @JsonProperty("userName") String userName,
                                       @JsonProperty("splits") List<List<String>> splits,
                                       @JsonProperty("hiveReadEntry") HiveReadEntry hiveReadEntry,
                                       @JsonProperty("splitClasses") List<String> splitClasses,
                                       @JsonProperty("columns") List<SchemaPath> columns,
                                       @JsonProperty("hiveStoragePluginConfig") HiveStoragePluginConfig hiveStoragePluginConfig)
      throws IOException, ExecutionSetupException, ReflectiveOperationException {
    super(registry, userName, splits, hiveReadEntry, splitClasses, columns, hiveStoragePluginConfig);
  }

  public HiveDrillNativeParquetSubScan(final HiveSubScan subScan)
      throws IOException, ExecutionSetupException, ReflectiveOperationException {
    super(subScan.getUserName(), subScan.getSplits(), subScan.getHiveReadEntry(), subScan.getSplitClasses(),
        subScan.getColumns(), subScan.getStoragePlugin());
  }
}
