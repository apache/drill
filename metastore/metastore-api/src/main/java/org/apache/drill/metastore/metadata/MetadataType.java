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
 * Enum with possible types of metadata.
 */
public enum MetadataType {

  /**
   * Metadata that can be applicable to any type.
   */
  ALL,

  /**
   * Table level metadata type.
   */
  TABLE,

  /**
   * Segment level metadata type. It corresponds to the metadata
   * within specific directory for FS tables, or may correspond to partition for hive tables.
   */
  SEGMENT,

  /**
   * Drill partition level metadata type. It corresponds to parts of table data which has the same
   * values within specific column, i.e. partitions discovered by Drill.
   */
  PARTITION,

  /**
   * File level metadata type.
   */
  FILE,

  /**
   * Row group level metadata type. Used for parquet tables.
   */
  ROW_GROUP,

  /**
   * Metadata type which helps to indicate that there is no overflow of metadata.
   */
  NONE
}
