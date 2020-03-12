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

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.store.easy.json.parser.ObjectListener;
import org.apache.drill.exec.vector.accessor.VariantWriter;

/**
 * Listener for a UNION type column which maps each JSON type to
 * the matching Drill type within the UNION. Used only if a column
 * is declared as UNION in the provided schema. This implementation
 * does not have a way to convert a non-UNION column into a UNION
 * during the scan. The reason is simple: the scan is obligated to
 * return a consistent schema. Converting a column between types,
 * especially after returning the first batch, will lead to an
 * inconsistent schema and to downstream schema change failures.
 */
public class VariantListener extends AbstractValueListener {

  private final VariantWriter writer;

  public VariantListener(JsonLoaderImpl loader, VariantWriter writer) {
    super(loader);
    this.writer = writer;
  }

  @Override
  public void onNull() { }

  @Override
  public void onBoolean(boolean value) {
    writer.scalar(MinorType.BIT).setBoolean(value);
  }

  @Override
  public void onInt(long value) {
    writer.scalar(MinorType.BIGINT).setLong(value);
  }

  @Override
  public void onFloat(double value) {
    writer.scalar(MinorType.FLOAT8).setDouble(value);
  }

  @Override
  public void onString(String value) {
    writer.scalar(MinorType.VARCHAR).setString(value);
  }

  @Override
  protected ColumnMetadata schema() {
    return writer.schema();
  }

  @Override
  public ObjectListener object() {
    return new VariantTupleListener(loader, writer);
  }

  private static class VariantTupleListener extends TupleListener {

    private final VariantWriter writer;

    public VariantTupleListener(JsonLoaderImpl loader, VariantWriter writer) {
      super(loader, writer.tuple(), null);
      this.writer = writer;
    }

    @Override
    public void onStart() {
      writer.setType(MinorType.MAP);
    }
  }
}
