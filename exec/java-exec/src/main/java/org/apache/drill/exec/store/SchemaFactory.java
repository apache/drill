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
package org.apache.drill.exec.store;

import org.apache.calcite.schema.SchemaPlus;

import org.apache.drill.exec.ops.QueryContext;

import java.io.IOException;

/**
 * StoragePlugins implements this interface to register the schemas they provide.
 */
public interface SchemaFactory {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SchemaFactory.class);

  /**
   * Register the schemas provided by this SchemaFactory implementation under the given parent schema.
   *
   * @param schemaConfig Configuration for schema objects.
   * @param parent Reference to parent schema.
   * @throws IOException
   */
  public void registerSchemas(SchemaConfig schemaConfig, SchemaPlus parent) throws IOException;
}
