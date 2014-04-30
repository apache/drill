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
package org.apache.drill.exec.planner.logical;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.ImmutableMap;
import org.apache.drill.exec.store.RecordWriter;
import org.apache.drill.exec.store.RecordWriterRegistry;
import org.apache.drill.exec.store.dfs.FileSystemConfig;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.util.Map;

/**
 * Implements <code>CreateTableEntry</code> interface to create new tables in FileSystem storage.
 */
@JsonTypeName("filesystem")
public class FileSystemCreateTableEntry implements CreateTableEntry {

  public FileSystemConfig config;
  public String format;
  public String location;

  /**
   * Create an entry.
   * @param config Storage configuration.
   * @param format Output format such as "csv", "parquet" etc.
   * @param location Directory where the data files for the new table are created.
   */
  public FileSystemCreateTableEntry(@JsonProperty("config") FileSystemConfig config,
                                    @JsonProperty("format") String format,
                                    @JsonProperty("location") String location) {
    this.config = config;
    this.format = format;
    this.location = location;
  }

  /**
   * Returns an implementation of the RecordWriter which is used to write data to new table.
   * @param prefix Fragment unique identifier to prefix to files created by RecordWriter.
   *               This is needed to avoid fragments overwriting file in parallel overwriting others.
   * @return A RecordWriter object.
   * @throws IOException
   */
  @Override
  public RecordWriter getRecordWriter(String prefix) throws IOException {
    Map<String, String> options = ImmutableMap.of(
        "location", location,
        FileSystem.FS_DEFAULT_NAME_KEY, config.connection,
        "prefix", prefix
    );

    return RecordWriterRegistry.get(format, options);
  }
}
