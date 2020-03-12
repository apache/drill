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

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.store.easy.json.parser.ArrayListener;
import org.apache.drill.exec.store.easy.json.parser.ValueDef;
import org.apache.drill.exec.vector.accessor.ObjectType;
import org.apache.drill.exec.vector.accessor.ObjectWriter;
import org.apache.drill.exec.vector.accessor.ScalarWriter;
import org.apache.drill.exec.vector.accessor.UnsupportedConversionError;

/**
 * Base class for scalar field listeners
 */
public abstract class ScalarListener extends AbstractValueListener {

  protected final ScalarWriter writer;
  protected final boolean isArray;

  public ScalarListener(JsonLoaderImpl loader, ScalarWriter writer) {
    super(loader);
    this.writer = writer;
    ColumnMetadata colSchema = writer.schema();
    isArray = colSchema.isArray();
  }

  public static ScalarListener listenerFor(JsonLoaderImpl loader, ObjectWriter colWriter) {
    ScalarWriter writer = colWriter.type() == ObjectType.ARRAY ?
        colWriter.array().scalar() : colWriter.scalar();
    switch (writer.schema().type()) {
      case BIGINT:
        return new BigIntListener(loader, writer);
      case BIT:
        return new BooleanListener(loader, writer);
      case FLOAT8:
        return new DoubleListener(loader, writer);
      case VARCHAR:
        return new VarCharListener(loader, writer);
      case DATE:
      case FLOAT4:
      case INT:
      case INTERVAL:
      case INTERVALDAY:
      case INTERVALYEAR:
      case SMALLINT:
      case TIME:
      case TIMESTAMP:
      case VARBINARY:
      case VARDECIMAL:
        // TODO: Implement conversions for above
      default:
        throw loader.buildError(
            UserException.internalError(null)
              .message("Unsupported JSON reader type: %s",
                  writer.schema().type().name()));
    }
  }

  @Override
  public ColumnMetadata schema() { return writer.schema(); }

  @Override
  public void onNull() {
    setNull();
  }

  protected void setNull() {
    try {
      if (isArray) {
        setArrayNull();
      } else {
        writer.setNull();
      }
    } catch (UnsupportedConversionError e) {
      throw loader.buildError(schema(),
          UserException.dataReadError()
            .message("Null value encountered in JSON input where Drill does not allow nulls."));
    }
  }

  protected abstract void setArrayNull();

  @Override
  public ArrayListener array(ValueDef valueDef) {
    if (isArray) {
      valueDef = new ValueDef(valueDef.type(), valueDef.dimensions() + 1);
    }
    throw loader.typeConversionError(schema(), valueDef);
  }
}
