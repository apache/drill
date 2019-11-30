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
package org.apache.drill.exec.store.easy.json.parser;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.store.easy.json.parser.JsonLoaderImpl.JsonElementParser;
import org.apache.drill.exec.vector.accessor.ArrayWriter;
import org.apache.drill.exec.vector.accessor.ObjectWriter;

import com.fasterxml.jackson.core.JsonToken;

/**
 * Parses [ ((value | null)(, (value | null))* )? ]<br>
 */

abstract class ArrayParser extends ContainerParser {

  /**
   * Parses [ value, value ... ]<br>
   * Where value is a scalar. The states for each value ensure that the
   * types are consistent (Drill does not support heterogeneous arrays.)
   */

  protected static class ScalarArrayParser extends ArrayParser {

    private final JsonElementParser scalarParser;

    public ScalarArrayParser(JsonLoaderImpl.JsonElementParser parent, String fieldName,
        ArrayWriter writer,
        JsonElementParser scalarParser) {
      super(parent, fieldName, writer);
      this.scalarParser = scalarParser;
    }

    @Override
    protected void parseContents(JsonToken token) {
      scalarParser.parse();
    }
  }

  /**
   * Parses [ ((item | null)(, (item | null)* )? ]
   */

  protected static class ObjectArrayParser extends ArrayParser {

    private final ObjectParser objectParser;

    public ObjectArrayParser(JsonLoaderImpl.JsonElementParser parent, String fieldName,
        ArrayWriter writer, ObjectParser objectParser) {
      super(parent, fieldName, writer);
      this.objectParser = objectParser;
    }

    @Override
    protected void parseContents(JsonToken token) {
      objectParser.parse();
    }
  }

  protected final ArrayWriter writer;

  public ArrayParser(JsonElementParser parent, String fieldName,
      ArrayWriter writer) {
    super(parent, fieldName);
    this.writer = writer;
  }

  @Override
  public boolean parse() {
    JsonToken token = loader.tokenizer.requireNext();
    switch (token) {
    case VALUE_NULL:
      return true;
    case START_ARRAY:
      writer.setNull(false);
      break;
    default:
      throw loader.syntaxError(token);
    }

    for (;;) {
      token = loader.tokenizer.requireNext();
      switch (token) {
      case END_ARRAY:
        return true;

      default:
        loader.tokenizer.unget(token);
        parseContents(token);
        writer.save();
        break;
      }
    }
  }

  protected abstract void parseContents(JsonToken token);

  @Override
  public boolean isAnonymous() { return true; }

  @Override
  public ColumnMetadata schema() { return writer.schema(); }

  @Override
  protected ObjectWriter newWriter(String key, MinorType type,
      DataMode mode) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected JsonElementParser nullArrayParser(String key) {
    throw new UnsupportedOperationException();
  }
}
