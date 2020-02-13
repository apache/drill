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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonToken;

/**
 * The root parsers are special: they must detect EOF. Drill supports
 * top-level objects either enclosed in an array (which forms legal
 * JSON), or as a series JSON objects (which is a common, if not
 * entirely legal, form of JSON.)
 */
public abstract class RootParser implements ElementParser {
  protected static final Logger logger = LoggerFactory.getLogger(RootParser.class);

  private final JsonStructureParser structParser;
  protected final ObjectParser rootObject;

  public RootParser(JsonStructureParser structParser) {
    this.structParser = structParser;
    this.rootObject = new ObjectParser(this, structParser.rootListener());
  }

  public abstract boolean parseRoot(TokenIterator tokenizer);

  @Override
  public void parse(TokenIterator tokenizer) {
    throw new UnsupportedOperationException();
  }

  protected boolean parseRootObject(JsonToken token, TokenIterator tokenizer) {
    // Position: ^ ?
    switch (token) {
      case NOT_AVAILABLE:
        return false; // Should never occur

      case START_OBJECT:
        // Position: { ^
        rootObject.parse(tokenizer);
        break;

      default:
        // Position ~{ ^
        // Not a valid object.
        // Won't actually get here: the Jackson parser prevents it.
        throw errorFactory().syntaxError(token); // Nothing else is valid
    }
    return true;
  }

  protected ErrorFactory errorFactory() {
    return structParser.errorFactory();
  }

  @Override
  public ElementParser parent() { return null; }

  @Override
  public JsonStructureParser structParser() { return structParser; }

  public static class RootObjectParser extends RootParser {

    public RootObjectParser(JsonStructureParser structParser) {
      super(structParser);
    }

    @Override
    public boolean parseRoot(TokenIterator tokenizer) {
      JsonToken token = tokenizer.next();
      if (token == null) {
        // Position: EOF ^
        return false;
      } else {
        return parseRootObject(token, tokenizer);
      }
    }
  }

  public static class RootArrayParser extends RootParser {

    public RootArrayParser(JsonStructureParser structParser) {
      super(structParser);
    }

    @Override
    public boolean parseRoot(TokenIterator tokenizer) {
      JsonToken token = tokenizer.next();
      if (token == null) {
        // Position: { ... EOF ^
        // Saw EOF, but no closing ]. Warn and ignore.
        // Note that the Jackson parser won't let us get here;
        // it will have already thrown a syntax error.
        logger.warn("Failed to close outer array. {}",
            tokenizer.context());
        return false;
      } else if (token == JsonToken.END_ARRAY) {
        return false;
      } else {
        return parseRootObject(token, tokenizer);
      }
    }
  }
}
