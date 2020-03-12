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

import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.store.easy.json.parser.ArrayListener;
import org.apache.drill.exec.store.easy.json.parser.ValueDef;
import org.apache.drill.exec.store.easy.json.parser.ValueListener;
import org.apache.drill.exec.vector.accessor.ArrayWriter;
import org.apache.drill.exec.vector.accessor.ObjectWriter;

/**
 * Listener for the List vector writer. A List in Drill is essentially
 * a repeated Union.
 */
public class ListListener extends AbstractValueListener {

  private final ObjectWriter listWriter;
  private final ListArrayListener arrayListener;

  public ListListener(JsonLoaderImpl loader, ObjectWriter listWriter) {
    super(loader);
    this.listWriter = listWriter;
    arrayListener = new ListArrayListener(loader, listWriter.array());
  }

  @Override
  public void onNull() { }

  @Override
  protected ColumnMetadata schema() {
    return listWriter.schema();
  }

  @Override
  public ArrayListener array(ValueDef valueDef) {
    return arrayListener;
  }

  private static class ListArrayListener extends AbstractArrayListener {

    private final ArrayWriter listWriter;

    public ListArrayListener(JsonLoaderImpl loader, ArrayWriter listWriter) {
      super(loader, listWriter.schema(),
          new VariantListener(loader, listWriter.variant()));
      this.listWriter = listWriter;
    }

    @Override
    public void onElementStart() {
      // For list, must say that the entry is non-null to
      // record an empty list. {a: null} vs. {a: []}.
      listWriter.setNull(false);
    }

    @Override
    public ValueListener element(ValueDef valueDef) {
      return elementListener;
    }

    @Override
    public void onElementEnd() {
      listWriter.save();
    }
  }
}
