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

import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.store.easy.json.parser.AbstractParser.LeafParser;

import com.fasterxml.jackson.core.JsonToken;

/**
 * Parse and ignore an unprojected value. The parsing just "free wheels", we
 * care only about matching brackets, but not about other details.
 */

class DummyValueParser extends LeafParser {

  public DummyValueParser(JsonLoaderImpl.JsonElementParser parent, String fieldName) {
    super(parent, fieldName);
  }

  @Override
  public boolean parse() {
    JsonToken token = loader.tokenizer.requireNext();
    switch (token) {
    case START_ARRAY:
    case START_OBJECT:
      parseTail();
      break;

    case VALUE_NULL:
    case VALUE_EMBEDDED_OBJECT:
    case VALUE_FALSE:
    case VALUE_TRUE:
    case VALUE_NUMBER_FLOAT:
    case VALUE_NUMBER_INT:
    case VALUE_STRING:
      break;

    default:
      throw loader.syntaxError(token);
    }
    return true;
  }

  public void parseTail() {

    // Parse (field: value)* }

    for (;;) {
      JsonToken token = loader.tokenizer.requireNext();
      switch (token) {

      // Not exactly precise, but the JSON parser handles the
      // details.

      case END_OBJECT:
      case END_ARRAY:
        return;

      case START_OBJECT:
      case START_ARRAY:
        parseTail(); // Recursively ignore objects
        break;

      default:
        break; // Ignore all else
      }
    }
  }

  @Override
  public ColumnMetadata schema() { return null; }
}
