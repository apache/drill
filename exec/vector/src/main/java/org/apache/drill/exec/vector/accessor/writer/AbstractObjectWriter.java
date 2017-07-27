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
package org.apache.drill.exec.vector.accessor.writer;

import org.apache.drill.exec.vector.accessor.ArrayWriter;
import org.apache.drill.exec.vector.accessor.ColumnWriterIndex;
import org.apache.drill.exec.vector.accessor.ObjectWriter;
import org.apache.drill.exec.vector.accessor.ScalarWriter;
import org.apache.drill.exec.vector.accessor.TupleWriter;

public abstract class AbstractObjectWriter implements ObjectWriter, WriterEvents {

  public abstract void bindIndex(ColumnWriterIndex index);

  @Override
  public ScalarWriter scalar() {
    throw new UnsupportedOperationException();
  }

  @Override
  public TupleWriter tuple() {
    throw new UnsupportedOperationException();
  }

  @Override
  public ArrayWriter array() {
    throw new UnsupportedOperationException();
  }

  @Override public void startWrite() { }
  @Override public void startValue() { }
  @Override public void endValue() { }
  @Override public void endWrite() { }
}
