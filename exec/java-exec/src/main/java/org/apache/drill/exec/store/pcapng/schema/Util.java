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
package org.apache.drill.exec.store.pcapng.schema;

import org.apache.drill.exec.vector.IntVector;
import org.apache.drill.exec.vector.NullableBigIntVector;
import org.apache.drill.exec.vector.NullableIntVector;
import org.apache.drill.exec.vector.NullableVarCharVector;
import org.apache.drill.exec.vector.TimeStampVector;
import org.apache.drill.exec.vector.ValueVector;

import static java.nio.charset.StandardCharsets.UTF_8;

public class Util {
  static void setNullableIntegerColumnValue(final int data, final ValueVector vv, final int count) {
    ((NullableIntVector.Mutator) vv.getMutator())
        .setSafe(count, data);
  }

  static void setIntegerColumnValue(final int data, final ValueVector vv, final int count) {
    ((IntVector.Mutator) vv.getMutator())
        .setSafe(count, data);
  }

  static void setTimestampColumnValue(final long data, final ValueVector vv, final int count) {
    ((TimeStampVector.Mutator) vv.getMutator())
        .setSafe(count, data / 1000);
  }

  static void setNullableLongColumnValue(final long data, final ValueVector vv, final int count) {
    ((NullableBigIntVector.Mutator) vv.getMutator())
        .setSafe(count, data);
  }

  static void setNullableStringColumnValue(final String data, final ValueVector vv, final int count) {
    ((NullableVarCharVector.Mutator) vv.getMutator())
        .setSafe(count, data.getBytes(UTF_8), 0, data.length());
  }

  static void setNullableBooleanColumnValue(final boolean data, final ValueVector vv, final int count) {
    ((NullableIntVector.Mutator) vv.getMutator())
        .setSafe(count, data ? 1 : 0);
  }
}