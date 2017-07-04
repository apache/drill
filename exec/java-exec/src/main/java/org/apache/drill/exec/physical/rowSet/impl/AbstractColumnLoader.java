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
package org.apache.drill.exec.physical.rowSet.impl;

import java.math.BigDecimal;

import org.apache.drill.exec.physical.rowSet.ColumnLoader;
import org.joda.time.Period;

/**
 * Abstract base class to provide generic value mapping.
 */

public abstract class AbstractColumnLoader implements ColumnLoader {

  @Override
  public void set(Object value) {
    if (value == null) {
      setNull();
    } else if (value instanceof Integer) {
      setInt((Integer) value);
    } else if (value instanceof Long) {
      setLong((Long) value);
    } else if (value instanceof String) {
      setString((String) value);
    } else if (value instanceof BigDecimal) {
      setDecimal((BigDecimal) value);
    } else if (value instanceof Period) {
      setPeriod((Period) value);
    } else if (value instanceof byte[]) {
      byte byteValue[] = (byte[]) value;
      setBytes(byteValue, byteValue.length);
    } else if (value instanceof Byte) {
      setInt((Byte) value);
    } else if (value instanceof Short) {
      setInt((Short) value);
    } else if (value instanceof Double) {
      setDouble((Double) value);
    } else if (value instanceof Float) {
      setDouble((Float) value);
    } else if (value.getClass().getName().startsWith("[")) {
      array().setArray(value);
    } else {
      throw new IllegalArgumentException("Unsupported type " +
                value.getClass().getSimpleName());
    }
  }
}
