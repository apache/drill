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
package org.apache.drill.exec.store.easy.json.loader;

import org.apache.drill.exec.vector.accessor.ScalarWriter;

/**
 * Value listener for JSON string values. Allows conversion from
 * other scalar types using the Java {@code toString()} semantics.
 * Use the "text-mode" hint in a provided schema to get the literal
 * JSON value.
 */
public class VarCharListener extends ScalarListener {

  private final boolean classicArrayNulls;

  public VarCharListener(JsonLoaderImpl loader, ScalarWriter writer) {
    super(loader, writer);
    classicArrayNulls = isArray ? loader.options().classicArrayNulls : false;
  }

  @Override
  public void onBoolean(boolean value) {
    writer.setString(Boolean.toString(value));
  }

  @Override
  public void onInt(long value) {
    writer.setString(Long.toString(value));
  }

  @Override
  public void onFloat(double value) {
    writer.setString(Double.toString(value));
  }

  @Override
  public void onString(String value) {
    writer.setString(value);
  }

  @Override
  protected void setArrayNull() {
    writer.setString(classicArrayNulls ? "null" : "");
  }
}
