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
package org.apache.drill.exec.physical.impl.scan.file;

import org.apache.drill.exec.physical.impl.scan.project.VectorSource;

public class FileMetadataColumn extends MetadataColumn {

  public static final int ID = 15;

  private final FileMetadataColumnDefn defn;

  /**
   * Constructor for resolved column.
   *
   * @param name
   * @param defn
   * @param fileInfo
   * @param projection
   */
  public FileMetadataColumn(String name, FileMetadataColumnDefn defn,
      FileMetadata fileInfo, VectorSource source, int sourceIndex) {
    super(name, defn.dataType(), defn.defn.getValue(fileInfo.filePath()), source, sourceIndex);
    this.defn = defn;
  }

  /**
   * Constructor for unresolved column.
   *
   * @param name
   * @param defn
   */

  public FileMetadataColumn(String name, FileMetadataColumnDefn defn) {
    super(name, defn.dataType(), null, null, 0);
    this.defn = defn;
  }

  @Override
  public int nodeType() { return ID; }

  public FileMetadataColumnDefn defn() { return defn; }

  @Override
  public MetadataColumn resolve(FileMetadata fileInfo, VectorSource source, int sourceIndex) {
    return new FileMetadataColumn(name(), defn, fileInfo, source, sourceIndex);
  }
}
