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
package org.apache.drill.exec.record.metadata.schema;

import org.apache.drill.exec.store.StorageStrategy;

import java.util.Map;

/**
 * Is used to provide schema when passed using table function.
 */
public class InlineSchemaProvider implements SchemaProvider {

  private final String schema;
  private final Map<String, String> properties;

  public InlineSchemaProvider(String schema, Map<String, String> properties) {
    this.schema = schema;
    this.properties = properties;
  }

  @Override
  public void delete() {
    throw new UnsupportedOperationException("Schema deletion is not supported");
  }

  @Override
  public void store(String schema, Map<String, String> properties, StorageStrategy storageStrategy) {
    throw new UnsupportedOperationException("Schema storage is not supported");
  }

  @Override
  public SchemaContainer read() {
    return new SchemaContainer(null, schema, properties);
  }

  @Override
  public boolean exists() {
    return true;
  }

}
