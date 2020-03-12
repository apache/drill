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
 * Listener for JSON Boolean fields. Allows conversion from numeric
 * fields (with the usual semantics: 0 = false, ~0 = true) and from
 * strings (using Java Boolean parsing semantics.)
 */
public class BooleanListener extends ScalarListener {

  public BooleanListener(JsonLoaderImpl loader, ScalarWriter writer) {
    super(loader, writer);
  }

  @Override
  public void onBoolean(boolean value) {
    writer.setBoolean(value);
  }

  @Override
  public void onInt(long value) {
    writer.setBoolean(value != 0);
  }

  @Override
  public void onFloat(double value) {
    writer.setBoolean(value != 0);
  }

  @Override
  public void onString(String value) {
    value = value.trim();
    if (value.isEmpty()) {
      setNull();
    } else {
      writer.setBoolean(Boolean.parseBoolean(value.trim()));
    }
  }

  @Override
  protected void setArrayNull() {
    writer.setBoolean(false);
  }
}
