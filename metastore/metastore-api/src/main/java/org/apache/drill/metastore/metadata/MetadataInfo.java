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
package org.apache.drill.metastore.metadata;

/**
 * Class that specifies metadata type and metadata information
 * which will be used for obtaining specific metadata from metastore.
 *
 * For example, for table-level metadata, it will be
 * {@code MetadataInfo(MetadataType.TABLE, MetadataInfo.GENERAL_INFO_KEY, null)}.
 */
public class MetadataInfo {

  public static final String GENERAL_INFO_KEY = "GENERAL_INFO";
  public static final String DEFAULT_SEGMENT_KEY = "DEFAULT_SEGMENT";
  public static final String DEFAULT_COLUMN_PREFIX = "_$SEGMENT_";

  private final MetadataType type;
  private final String key;
  private final String identifier;

  public MetadataInfo(MetadataType type, String key, String identifier) {
    this.type = type;
    this.key = key;
    this.identifier = identifier;
  }

  public MetadataType getType() {
    return type;
  }

  public String getKey() {
    return key;
  }

  public String getIdentifier() {
    return identifier;
  }
}
