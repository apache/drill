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
import org.apache.drill.exec.store.easy.json.parser.JsonLoaderImpl.NullTypeMarker;
import org.apache.drill.exec.vector.accessor.ArrayWriter;
import org.apache.drill.exec.vector.accessor.ObjectWriter;
import org.apache.drill.exec.vector.accessor.writer.RepeatedListWriter;

import com.fasterxml.jackson.core.JsonToken;

/**
 * Parses [ ([ .*)? ]
 */

class RepeatedListParser extends ArrayParser {

  protected static class NullInnerArrayParser extends AbstractParser.LeafParser implements NullTypeMarker
  {
    private final RepeatedListParser listParser;

    public NullInnerArrayParser(RepeatedListParser parentState, String key) {
      super(parentState, key);
      this.listParser = parentState;
      loader.addNullMarker(this);
    }

    /**
     * Parse null | [] | ([ some_token)? ]
     */

    @Override
    public boolean parse() {
      // Position: [ ^
      JsonToken token = parseEmptyEntry();
      if (token != JsonToken.START_ARRAY) {
        return true;
      }

      JsonElementParser newParser = detectChildType();
      // Position: [ [ ^ ?
      loader.tokenizer.unget(token);
      // Position: [ ^ [ ?
      listParser.replaceChild(key(), newParser);
      loader.removeNullMarker(this);
      return newParser.parse();
    }

    private JsonToken parseEmptyEntry() {

      // Parse null* [|]

      JsonToken token = loader.tokenizer.requireNext();
      // Position: [ ? ^
      switch (token) {
      case VALUE_NULL:

        // Null value in the outer array is
        // the same as an empty inner array.

        // Position: [ null ^
        break;

      case START_ARRAY:
        JsonToken token2 = loader.tokenizer.requireNext();
        // Position: [ [ ? ^
        if (token2 == JsonToken.END_ARRAY) {
          // Position: [ [ ] ^
          return token2;
        }
        loader.tokenizer.unget(token2);
        // Position: [ [ ^ ?
        break;

      case END_ARRAY:
        loader.tokenizer.unget(token);
        // Position: [ (null | [])* ^ ]
        break;

      default:
        throw loader.syntaxError(token);
      }
      return token;
    }

    /**
     * Detect the type of a nested array. We've seen [[ so far.
     * The next token is not ]; it should tell us the kind of
     * the inner array.
     *
     * @param key field name
     * @return parser for the list
     */

    private JsonElementParser detectChildType() {
      JsonToken token = loader.tokenizer.peek();
      // Position: [ [ ^ ?
      switch (token) {
      case VALUE_NULL:
      case END_ARRAY:

        // Should have been handled above.

        throw new IllegalStateException();
      case START_ARRAY:
        // Position: [ [ [ ^
        return listParser.detectArrayParser(key());
      case START_OBJECT:
        // Position: [ [ { ^
        return listParser.objectArrayParser(key());
      default:
        // Position: [ [ scalar ^
        return listParser.detectScalarArrayParser(token, key());
      }
    }

    @Override
    public void forceResolution() {

      // This is a tough one. We've seen [[]]] or [[null]] to any depth.
      // (That is, we may have seen [[[]]] or [[[[null]]]].)
      // But, we don't know the type of the inner element. It could well be another
      // array. We're at the end of the batch and must choose something.
      // Let's choose array of Varchar since that will match either strings
      // or all text mode. We'll force the field into text mode, but this works
      // only if the contents turn out to be scalars, and the same decision is
      // made in other files (which is very unlikely.)
      // This is why JSON needs a schema. It is naive to think Drill can parse
      // arbitrary JSON without a schema.

      JsonLoaderImpl.logger.warn("Ambiguous type! JSON repeated array {}" +
          " contains all nulls. Assuming text mode.",
          fullName());
      listParser.replaceChild(key(), listParser.textArrayParser(key()));
      loader.removeNullMarker(this);
    }

    @Override
    public ColumnMetadata schema() { return null; }
  }

  private JsonElementParser childParser;

  public RepeatedListParser(JsonElementParser parent, String key, ArrayWriter writer) {
    super(parent, key, writer);
    childParser = new NullInnerArrayParser(this, key);
  }

  @Override
  protected void parseContents(JsonToken token) {
    childParser.parse();
  }

  @Override
  protected ObjectWriter newWriter(String key, MinorType type,
      DataMode mode) {
    assert mode == DataMode.REPEATED;
    RepeatedListWriter listWriter = (RepeatedListWriter) writer;
    return listWriter.defineElement(schemaFor(writer.schema().name(), type, mode));
  }

  @Override
  protected JsonElementParser nullArrayParser(String key) {
    throw new IllegalStateException();
  }

  @Override
  protected void replaceChild(String key, JsonElementParser newParser) {
    childParser = newParser;
  }
}
