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
package org.apache.drill.exec.store.accumulo;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;

/**
 * Constants used by the Accumulo storage plugin.
 */
public interface DrillAccumuloConstants {

  /**
   * Name of the row key column in Drill queries.
   */
  String ROW_KEY = "row_key";

  /**
   * Schema path for the row key.
   */
  SchemaPath ROW_KEY_PATH = SchemaPath.getSimplePath(ROW_KEY);

  /**
   * Type for the row key column (required VARBINARY).
   */
  MajorType ROW_KEY_TYPE = Types.required(MinorType.VARBINARY);

  /**
   * Type for column family maps (required MAP).
   */
  MajorType COLUMN_FAMILY_TYPE = Types.required(MinorType.MAP);

  /**
   * Type for individual columns within a family (optional VARBINARY).
   */
  MajorType COLUMN_TYPE = Types.optional(MinorType.VARBINARY);

  /**
   * Separator between column family and qualifier in Accumulo keys.
   */
  String COLUMN_SEPARATOR = ":";

  /**
   * Default batch size for scanner caching.
   */
  int DEFAULT_BATCH_SIZE = 4000;
}
