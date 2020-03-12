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
 * Listener for JSON integer values. Allows conversion from
 * Boolean, double and string types. (The conversion from double
 * is lossy, but perhaps better than failing the query.)
 * Conversion from Boolean is the usual semantics:
 * true = 1, false = 0.  Conversion from string uses the Java
 * integer parsing semantics.
 */
public class BigIntListener extends ScalarListener {

  public BigIntListener(JsonLoaderImpl loader, ScalarWriter writer) {
    super(loader, writer);
  }

  @Override
  public void onBoolean(boolean value) {
    writer.setLong(value ? 1 : 0);
  }

  @Override
  public void onInt(long value) {
    writer.setLong(value);
  }

  @Override
  public void onFloat(double value) {
    writer.setLong(Math.round(value));
  }

  @Override
  public void onString(String value) {
    value = value.trim();
    if (value.isEmpty()) {
      setNull();
    } else {
      try {
        writer.setLong(Long.parseLong(value));
      } catch (NumberFormatException e) {
        throw loader.dataConversionError(schema(), "string", value);
      }
    }
  }

  @Override
  protected void setArrayNull() {
    writer.setLong(0);
  }
}
