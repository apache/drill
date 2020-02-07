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
import java.io.InputStream;

import org.apache.drill.exec.store.easy.json.parser.RootParser.RootArrayParser;
import org.apache.drill.exec.store.easy.json.parser.RootParser.RootObjectParser;
import org.apache.drill.exec.store.easy.json.parser.TokenIterator.RecoverableJsonException;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Parser for JSON that converts a stream of tokens from the Jackson JSON
 * parser into a set of events on listeners structured to follow the
 * data structure of the incoming data. JSON can assume many forms. This
 * class assumes that the data is in a tree structure that corresponds
 * to the Drill row structure: a series of object with (mostly) the
 * same schema. Members of the top-level object can be Drill types:
 * scalars, arrays, nested objects (Drill "MAP"s), and so on.
 * <p>
 * The structure parser follows the structure of the incoming data,
 * whatever it might be. This class imposes no semantic rules on that
 * data, it just "calls 'em as it sees 'em" as they say. The listeners
 * are responsible for deciding if the data data makes sense, and if
 * so, how it should be handled.
 * <p>
 * The root listener will receive an event to fields in the top-level
 * object as those fields first appear. Each field is a value object
 * and can correspond to a scalar, array, another object, etc. The
 * type of the value is declared when known, but sometimes it is not
 * known, such as if the value is {@code null}. And, of course, according
 * to JSON, the value is free to change from one row to the next. The
 * listener decides it if wants to handle such "schema change", and if
 * so, how.
 */
public class JsonStructureParser {
  protected static final Logger logger = LoggerFactory.getLogger(JsonStructureParser.class);

  private final JsonParser parser;
  private final JsonStructureOptions options;
  private final ObjectListener rootListener;
  private final ErrorFactory errorFactory;
  private final TokenIterator tokenizer;
  private final RootParser rootState;
  private int errorRecoveryCount;

  /**
   * Constructor for the structure parser.
   *
   * @param stream the source of JSON text
   * @param options configuration options for the Jackson JSON parser
   * and this structure parser
   * @param rootListener listener for the top-level objects in the
   * JSON stream
   * @param errorFactory factory for errors thrown for various
   * conditions
   */
  public JsonStructureParser(InputStream stream, JsonStructureOptions options,
      ObjectListener rootListener, ErrorFactory errorFactory) {
    this.options = Preconditions.checkNotNull(options);
    this.rootListener = Preconditions.checkNotNull(rootListener);
    this.errorFactory = Preconditions.checkNotNull(errorFactory);
    try {
      ObjectMapper mapper = new ObjectMapper()
          .configure(JsonParser.Feature.ALLOW_COMMENTS, true)
          .configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true)
          .configure(JsonParser.Feature.ALLOW_NON_NUMERIC_NUMBERS, options.allowNanInf);

      parser = mapper.getFactory().createParser(stream);
    } catch (JsonParseException e) {
      throw errorFactory().parseError("Failed to create the JSON parser", e);
    } catch (IOException e) {
      throw errorFactory().ioException(e);
    }
    tokenizer = new TokenIterator(parser, options, errorFactory());
    rootState = makeRootState();
  }

  public JsonStructureOptions options() { return options; }
  public ErrorFactory errorFactory() { return errorFactory; }
  public ObjectListener rootListener() { return rootListener; }

  private RootParser makeRootState() {
    JsonToken token = tokenizer.next();
    if (token == null) {
      return null;
    }
    switch (token) {

      // File contains an array of records.
      case START_ARRAY:
        if (options.skipOuterList) {
          return new RootArrayParser(this);
        } else {
          throw errorFactory().structureError(
              "JSON includes an outer array, but outer array support is not enabled");
        }

      // File contains a sequence of one or more records,
      // presumably sequentially.
      case START_OBJECT:
        tokenizer.unget(token);
        return new RootObjectParser(this);

      // Not a valid JSON file for Drill.
      // Won't get here because the Jackson parser catches errors.
      default:
        throw errorFactory().syntaxError(token);
    }
  }

  public boolean next() {
    if (rootState == null) {
      // Only occurs for an empty document
      return false;
    }
    for (;;) {
      try {
        return rootState.parseRoot(tokenizer);
      } catch (RecoverableJsonException e) {
        if (! recover()) {
          return false;
        }
      }
    }
  }

  /**
   * Attempt recovery from a JSON syntax error by skipping to the next
   * record. The Jackson parser is quite limited in its recovery abilities.
   *
   * @return {@code true}  if another record can be read, {@code false}
   * if EOF.
   * @throws UserException if the error is unrecoverable
   * @see <a href="https://issues.apache.org/jira/browse/DRILL-4653">DRILL-4653</a>
   * @see <a href="https://issues.apache.org/jira/browse/DRILL-5953">DRILL-5953</a>
   */
  private boolean recover() {
    logger.warn("Attempting recovery from JSON syntax error. " + tokenizer.context());
    boolean firstAttempt = true;
    for (;;) {
      for (;;) {
        try {
          if (parser.isClosed()) {
            throw errorFactory().unrecoverableError();
          }
          JsonToken token = tokenizer.next();
          if (token == null) {
            if (firstAttempt) {
              throw errorFactory().unrecoverableError();
            }
            return false;
          }
          if (token == JsonToken.NOT_AVAILABLE) {
            return false;
          }
          if (token == JsonToken.END_OBJECT) {
            break;
          }
          firstAttempt = false;
        } catch (RecoverableJsonException e) {
          // Ignore, keep trying
        }
      }
      try {
        JsonToken token = tokenizer.next();
        if (token == null || token == JsonToken.NOT_AVAILABLE) {
          return false;
        }
        if (token == JsonToken.START_OBJECT) {
          logger.warn("Attempting to resume JSON parse. " + tokenizer.context());
          tokenizer.unget(token);
          errorRecoveryCount++;
          return true;
        }
      } catch (RecoverableJsonException e) {
        // Ignore, keep trying
      }
    }
  }

  public int recoverableErrorCount() { return errorRecoveryCount; }

  public void close() {
    if (errorRecoveryCount > 0) {
      logger.warn("Read JSON input with {} recoverable error(s).",
          errorRecoveryCount);
    }
    try {
      parser.close();
    } catch (IOException e) {
      logger.warn("Ignored failure when closing JSON source", e);
    }
  }
}
