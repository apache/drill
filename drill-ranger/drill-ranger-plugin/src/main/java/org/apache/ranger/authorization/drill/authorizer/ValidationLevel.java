/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ranger.authorization.drill.authorizer;

/**
 * Resource validation level enum.
 * Controls the depth of validation in the validateResource method.
 */
public enum ValidationLevel {
  /** Validate up to the datasource level (user, dataSource). */
  DATASOURCE,
  /** Validate up to the schema level (user, dataSource, schema). */
  SCHEMA,
  /** Validate up to the table level (user, dataSource, schema, table). */
  TABLE,
  /** Full validation including columns (user, dataSource, schema, table, columns). */
  COLUMN
}
