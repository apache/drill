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
package org.apache.drill.exec.record.metadata;

import org.apache.drill.common.types.TypeProtos;

/**
 * A common interface shared by SchemaBuilder and MapBuilder allowing one to do most
 * operations for constructing metadata for hierarchical data
 * without having to keep track of whether you are dealing with the top-level schema/row
 * level or a map within it.
 */
public interface MapBuilderLike {

  MapBuilderLike addArray(String colName, TypeProtos.MinorType drillType);

  MapBuilderLike addNullable(String colName, TypeProtos.MinorType drillType);

  MapBuilderLike add(String colName, TypeProtos.MinorType drillType);

  MapBuilderLike addMapArray(String colName);

  MapBuilderLike addMap(String colName);

  void resume();
}
