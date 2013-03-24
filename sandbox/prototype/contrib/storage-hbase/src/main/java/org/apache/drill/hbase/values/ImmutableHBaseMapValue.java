/*******************************************************************************
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
 ******************************************************************************/
package org.apache.drill.hbase.values;

import org.apache.drill.exec.ref.values.BaseArrayValue;
import org.apache.drill.exec.ref.values.BaseMapValue;
import org.apache.drill.exec.ref.values.DataValue;

import java.util.Iterator;
import java.util.Map;

public abstract class ImmutableHBaseMapValue extends BaseMapValue {

  public abstract Iterator<Map.Entry<CharSequence, DataValue>> iterator();

  public abstract boolean equals(DataValue v);

  public abstract DataValue copy();

  protected abstract DataValue getByName(CharSequence name);

  @Override
  public final BaseMapValue getAsMap() {
    return this;
  }

  @Override
  protected final void removeByName(CharSequence name) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected final void setByName(CharSequence name, DataValue v) {
    throw new UnsupportedOperationException();
  }

  @Override
  public BaseArrayValue getAsArray() {
    throw new UnsupportedOperationException();
  }

}
