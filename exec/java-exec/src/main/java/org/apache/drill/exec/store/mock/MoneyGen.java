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
package org.apache.drill.exec.store.mock;

import java.util.Random;

import org.apache.drill.exec.vector.Float8Vector;
import org.apache.drill.exec.vector.ValueVector;

/**
 * Generates a mock money field as a double over the range 0
 * to 1 million. Values include cents. That is the value
 * ranges uniformly over the range 0.00 to
 * 999,999.99.
 */

public class MoneyGen implements FieldGen {

  private final Random rand = new Random();

  @Override
  public void setup(ColumnDef colDef) { }

  private double value() {
    return Math.ceil(rand.nextDouble() * 1_000_000 * 100) / 100;
  }

  @Override
  public void setValue(ValueVector v, int index) {
    Float8Vector vector = (Float8Vector) v;
    vector.getMutator().set(index, value());
  }
}
