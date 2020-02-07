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

import java.io.IOException;

import org.apache.drill.exec.vector.accessor.UnsupportedConversionError;

import com.fasterxml.jackson.core.JsonLocation;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

public class TokenIterator {
  public static final int MAX_LOOKAHEAD = 30;

  /**
   * Internal exception to unwind the stack when a syntax
   * error is detected within a record. Allows for recovery.
   */
  @SuppressWarnings("serial")
  class RecoverableJsonException extends RuntimeException {
  }

  private final JsonParser parser;
  private final JsonStructureOptions options;
  private final ErrorFactory errorFactory;
  private final JsonToken[] lookahead = new JsonToken[MAX_LOOKAHEAD];
  private int count;

  public TokenIterator(JsonParser parser, JsonStructureOptions options, ErrorFactory errorFactory) {
    this.parser = parser;
    this.options = options;
    this.errorFactory = errorFactory;
  }

  public ErrorFactory errorFactory() { return errorFactory; }

  public JsonToken next() {
    if (count > 0) {
      return lookahead[--count];
    }
    try {
      return parser.nextToken();
    } catch (JsonParseException e) {
      if (options.skipMalformedRecords) {
        throw new RecoverableJsonException();
      } else {
        throw errorFactory.syntaxError(e);
      }
    } catch (IOException e) {
      throw errorFactory.ioException(e);
    }
  }

  public String context() {
    JsonLocation location = parser.getCurrentLocation();
    String token;
    try {
      token = parser.getText();
    } catch (IOException e) {
      token = "<unknown>";
    }
    return new StringBuilder()
        .append("line ")
        .append(location.getLineNr())
        .append(", column ")
        .append(location.getColumnNr())
        .append(", near token \"")
        .append(token)
        .append("\"")
        .toString();
  }

  public JsonToken requireNext() {
    JsonToken token = next();
    if (token == null) {
      throw errorFactory.structureError("Premature EOF of JSON file");
    }
    return token;
  }

  public JsonToken peek() {
    JsonToken token = requireNext();
    unget(token);
    return token;
  }

  public void unget(JsonToken token) {
    if (count == lookahead.length) {
      throw errorFactory.structureError(
          String.format("Excessive JSON array nesting. Max allowed: %d", lookahead.length));
    }
    lookahead[count++] = token;
  }

  public String textValue() {
    try {
      return parser.getText();
    } catch (IOException e) {
      throw errorFactory.ioException(e);
    }
  }

  public long longValue() {
    try {
      return parser.getLongValue();
    } catch (IOException e) {
      throw errorFactory.ioException(e);
    } catch (UnsupportedConversionError e) {
      throw errorFactory.typeError(e);
    }
  }

  public String stringValue() {
    try {
      return parser.getValueAsString();
    } catch (IOException e) {
      throw errorFactory.ioException(e);
    } catch (UnsupportedConversionError e) {
      throw errorFactory.typeError(e);
    }
  }

  public double doubleValue() {
    try {
      return parser.getValueAsDouble();
    } catch (IOException e) {
      throw errorFactory.ioException(e);
    } catch (UnsupportedConversionError e) {
      throw errorFactory.typeError(e);
    }
  }
}
