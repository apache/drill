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

import org.apache.drill.exec.vector.IntVector;
import org.apache.drill.exec.vector.ValueVector;

/**
 * Generates integer values uniformly randomly distributed over
 * the entire 32-bit integer range from
 * {@link Integer.MIN_VALUE} to {@link Integer.MAX_VALUE}.
 */

public class IntGen implements FieldGen {

  private final Random rand = new Random();

  @Override
  public void setup(ColumnDef colDef) { }

  private int value() {
    return rand.nextInt();
  }

  @Override
  public void setValue(ValueVector v, int index) {
    IntVector vector = (IntVector) v;
    vector.getMutator().set(index, value());
  }
}
