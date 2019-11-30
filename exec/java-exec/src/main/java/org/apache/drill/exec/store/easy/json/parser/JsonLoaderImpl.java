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
import java.util.ArrayList;
import java.util.List;

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.resultSet.RowSetLoader;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.store.easy.json.JsonLoader;
import org.apache.drill.exec.vector.accessor.ObjectWriter;
import org.apache.drill.exec.vector.accessor.UnsupportedConversionError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Revised JSON loader that is based on the
 * {@link ResultSetLoader} abstraction. Represents the JSON parse as a
 * set of parse states, each of which represents some point in the traversal
 * of the JSON syntax. Parse nodes also handle options such as all text mode
 * vs. type-specific parsing. Think of this implementation as a
 * recursive-descent parser, with the parser itself discovered and
 * "compiled" on the fly as we see incoming data.
 * <p>
 * Actual parsing is handled by the Jackson parser class. The input source is
 * represented as an {@link InputStream} so that this mechanism can parse files
 * or strings.
 * <p>
 * The JSON parser mechanism runs two state machines intertwined:
 * <ol>
 * <li>The actual parser (to parse each JSON object, array or scalar according
 * to its inferred type.</li>
 * <li>The type discovery machine, which is made complex because JSON may include
 * long runs of nulls. (See below.)</li>
 * </ol>
 *
 * <h4>Schema Discovery</h4>
 *
 * Fields are discovered on the fly. Types are inferred from the first JSON token
 * for a field. Type inference is less than perfect: it cannot handle type changes
 * such as first seeing 10, then 12.5, or first seeing "100", then 200.
 * <p>
 * When a field first contains null or an empty list, "null deferral" logic
 * adds a special state that "waits" for an actual data type to present itself.
 * This allows the parser to handle a series of nulls, empty arrays, or arrays
 * of nulls (when using lists) at the start of the file. If no type ever appears,
 * the loader forces the field to "text mode", hoping that the field is scalar.
 * <p>
 * To slightly help the null case, if the projection list shows that a column
 * must be an array or a map, then that information is used to guess the type
 * of a null column.
 * <p>
 * The code includes a prototype mechanism to provide type hints for columns.
 * At present, it is just used to handle nulls that are never "resolved" by the
 * end of a batch. Would be much better to use the hints (or a full schema) to
 * avoid the huge mass of code needed to handle nulls.
 *
 * <h4>Comparison to Original JSON Reader</h4>
 *
 * This class replaces the {@link JsonReader} class used in Drill versions 1.12
 * and before. Compared with the previous version, this implementation:
 * <ul>
 * <li>Materializes parse states as classes rather than as methods and
 * boolean flags as in the prior version.</li>
 * <li>Reports errors as {@link UserException} objects, complete with context
 * information, rather than as generic Java exception as in the prior version.</li>
 * <li>Moves parse options into a separate {@link JsonOptions} class.</li>
 * <li>Iteration protocol is simpler: simply call {@link #next()} until it returns
 * <tt>false</tt>. Errors are reported out-of-band via an exception.</li>
 * <li>The result set loader abstraction is perfectly happy with an empty schema.
 * For this reason, this version (unlike the original) does not make up a dummy
 * column if the schema would otherwise be empty.</li>
 * <li>Projection pushdown is handled by the {@link ResultSetLoader} rather than
 * the JSON loader. This class always creates a vector writer, but the result set
 * loader will return a dummy (no-op) writer for non-projected columns.</li>
 * <li>Like the original version, this version "free wheels" over unprojected objects
 * and arrays; watching only for matching brackets, but ignoring all else.</li>
 * <li>Writes boolean values as SmallInt values, rather than as bits in the
 * prior version.</li>
 * <li>This version also "free-wheels" over all unprojected values. If the user
 * finds that they have inconsistent data in some field f, then the user can
 * project fields except f; Drill will ignore the inconsistent values in f.</li>
 * <li>Because of this free-wheeling capability, this version does not need a
 * "counting" reader; this same reader handles the case in which no fields are
 * projected for <tt>SEELCT COUNT(*)</tt> queries.</li>
 * <li>Runs of null values result in a "deferred null state" that patiently
 * waits for an actual value token to appear, and only then "realizes" a parse
 * state for that type.</li>
 * <li>Provides the same limited error recovery as the original version. See
 * <a href="https://issues.apache.org/jira/browse/DRILL-4653">DRILL-4653</a>
 * and
 * <a href="https://issues.apache.org/jira/browse/DRILL-5953">DRILL-5953</a>.
 * </li>
 * </ul>
 */

public class JsonLoaderImpl implements JsonLoader {

  protected static final Logger logger = LoggerFactory.getLogger(JsonLoaderImpl.class);

  public static final String ROOT_NAME = "<root>";

  public interface TypeNegotiator {
    MajorType typeOf(List<String> path);
  }

  public static class JsonOptions {
    public String context;
    public boolean allTextMode;
    public boolean extended = true;
    public boolean readNumbersAsDouble;

    /**
     * Allow Infinity and NaN for float values.
     */

    public boolean allowNanInf;

    /**
     * Describes whether or not this reader can unwrap a single root array record
     * and treat it like a set of distinct records.
     */
    public boolean skipOuterList = true;
    public boolean skipMalformedRecords;
    public boolean unionEnabled;

    public TypeNegotiator typeNegotiator;

    /**
     * By default, the JSON parser uses repeated types: either a repeated
     * scalar, repeated map or repeated list. If this flag is enabled, the
     * parser will use the experimental list vector type which can form
     * a list of any type (including lists), and allows array values to
     * be null. The code for lists existed in bits and pieces in Drill 1.12,
     * was extended to work for JSON in Drill 1.13, but remains to be
     * supported in the rest of Drill.
     */

    public boolean useListType;
    public boolean detectTypeEarly;
  }

  @SuppressWarnings("serial")
  static class RecoverableJsonException extends RuntimeException {
  }

  interface JsonElementParser {
    String key();
    JsonLoader loader();
    JsonElementParser parent();
    boolean isAnonymous();
    boolean parse();
    ColumnMetadata schema();
  }

  /**
   * Parses ^[ ... ]$
   */

  protected static class RootArrayParser extends ContainerParser {

    private final RowSetLoader rootWriter;
    private final ObjectParser rootTuple;

    public RootArrayParser(JsonLoaderImpl loader, RowSetLoader rootWriter) {
      super(loader, ROOT_NAME);
      this.rootWriter = rootWriter;
      rootTuple = new ObjectParser(this, key() + "[]", rootWriter);
    }

    @Override
    public boolean parse() {
      rootWriter.start();
      JsonToken token = loader.tokenizer.requireNext();
      if (token == JsonToken.END_ARRAY) {
        return false;
      }
      loader.tokenizer.unget(token);
      rootTuple.parse();
      return true;
    }

    @Override
    public ColumnMetadata schema() { return null; }

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

  /**
   * Parses:
   * <ul>
   * <li>^{ ... }$</li>
   * <li>^{ ... } { ... } ...$</li>
   * </ul>
   */

  protected static class RootTupleState extends ObjectParser {

    public RootTupleState(JsonLoaderImpl loader, RowSetLoader rootWriter) {
      super(loader, rootWriter);
    }

    @Override
    public boolean isAnonymous() { return true; }

    @Override
    public ColumnMetadata schema() { return null; }
  }

  interface NullTypeMarker {
    void forceResolution();
  }

  final JsonParser parser;
  private final RowSetLoader rootWriter;
  protected final JsonOptions options;
  protected final TokenIterator tokenizer;
  private JsonElementParser rootState;
  private int errorRecoveryCount;

  // Using a simple list. Won't perform well if we have hundreds of
  // null fields; but then we've never seen such a pathologically bad
  // case... Usually just one or two fields have deferred nulls.

  private final List<JsonLoaderImpl.NullTypeMarker> nullStates = new ArrayList<>();

  public JsonLoaderImpl(InputStream stream, RowSetLoader rootWriter, JsonOptions options) {
    try {
      ObjectMapper mapper = new ObjectMapper()
          .configure(JsonParser.Feature.ALLOW_COMMENTS, true)
          .configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
      if (options.allowNanInf) {
        mapper.configure(JsonParser.Feature.ALLOW_NON_NUMERIC_NUMBERS, true);
      }

      parser = mapper.getFactory().createParser(stream);
    } catch (JsonParseException e) {
      throw UserException
          .internalError(e)
          .addContext("Failed to create the JSON parser")
          .addContext("Source", options.context)
          .build(logger);
    } catch (IOException e) {
      throw ioException(e);
    }
    this.rootWriter = rootWriter;
    this.options = options;
    tokenizer = new TokenIterator(this);
    rootState = makeRootState();
  }

  private JsonElementParser makeRootState() {
    JsonToken token = tokenizer.next();
    if (token == null) {
      return null;
    }
    switch (token) {

    // File contains an array of records.

    case START_ARRAY:
      if (options.skipOuterList) {
        return new RootArrayParser(this, rootWriter);
      } else {
        throw UserException
          .dataReadError()
          .message("JSON includes an outer array, but outer array support is not enabled")
          .addContext("Location", tokenizer.context())
          .build(logger);
      }

    // File contains a sequence of one or more records,
    // presumably sequentially.

    case START_OBJECT:
      tokenizer.unget(token);
      return new RootTupleState(this, rootWriter);

    // Not a valid JSON file for Drill.

    default:
      throw syntaxError(token);
    }
  }

  @Override
  public boolean next() {
    if (rootState == null) {
      return false;
    }

    // From original code.
    // Does this ever actually occur?

    if (parser.isClosed()) {
      rootState = null;
      return false;
    }
    for (;;) {
      try {
        return rootState.parse();
      } catch (RecoverableJsonException e) {
        if (! recover()) {
          return false;
        }
      }
    }
  }

  public void addNullMarker(JsonLoaderImpl.NullTypeMarker marker) {
    nullStates.add(marker);
  }

  public void removeNullMarker(JsonLoaderImpl.NullTypeMarker marker) {
    nullStates.remove(marker);
  }

  /**
   * Finish reading a batch of data. We may have pending "null" columns:
   * a column for which we've seen only nulls, or an array that has
   * always been empty. The batch needs to finish, and needs a type,
   * but we still don't know the type. Since we must decide on one,
   * we do the following:
   * <ul>
   * <li>If given a type negotiator, ask it the type. Perhaps a
   * prior reader in the same scanner determined the type in a
   * previous file.</li>
   * <li>Guess Varchar, and switch to text mode.</li>
   * </ul>
   *
   * Note that neither of these choices is perfect. There is no guarantee
   * that prior files either were seen, or have the same type as this
   * file. Also, switching to text mode means results will vary
   * from run to run depending on the order that we see empty and
   * non-empty values for this column. Plus, since the system is
   * distributed, the decision made here may conflict with that made in
   * some other fragment.
   * <p>
   * The only real solution is for the user to provide schema
   * information to resolve the ambiguity; but Drill has no way to
   * gather that information at present.
   * <p>
   * Bottom line: the user is responsible for not giving Drill
   * ambiguous data that would require Drill to predict the future.
   */

  @Override
  public void endBatch() {
    List<NullTypeMarker> copy = new ArrayList<>();
    copy.addAll(nullStates);
    for (NullTypeMarker marker : copy) {
      marker.forceResolution();
    }
    assert nullStates.isEmpty();
  }

  /**
   * Attempt recovery from a JSON syntax error by skipping to the next
   * record. The Jackson parser is quite limited in its recovery abilities.
   *
   * @return <tt>true<tt> if another record can be read, <tt>false</tt>
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
            throw unrecoverableError();
          }
          JsonToken token = tokenizer.next();
          if (token == null) {
            if (firstAttempt) {
              throw unrecoverableError();
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

  private UserException unrecoverableError() {
    throw UserException
        .dataReadError()
        .message("Unrecoverable JSON syntax error.")
        .addContext("Location", tokenizer.context())
        .build(logger);
  }

  UserException syntaxError(JsonToken token, String context, String expected) {
    if (options.skipMalformedRecords) {
      throw new RecoverableJsonException();
    } else {
      return UserException
          .dataReadError()
          .message("JSON encountered a value of the wrong type")
          .message("Field", context)
          .message("Expected type", expected)
          .message("Actual token", token.toString())
          .addContext("Location", tokenizer.context())
          .build(logger);
    }
  }

  UserException syntaxError(JsonToken token) {
    return UserException
        .dataReadError()
        .message("JSON syntax error.")
        .addContext("Current token", token.toString())
        .addContext("Location", tokenizer.context())
        .build(logger);
  }

  UserException syntaxError(String message) {
    return UserException
        .dataReadError()
        .message(message)
        .addContext("Location", tokenizer.context())
        .build(logger);
  }

  UserException ioException(IOException e) {
    return UserException
        .dataReadError(e)
        .addContext("I/O error reading JSON")
        .addContext("Location", parser == null ? options.context : tokenizer.context())
        .build(logger);
  }

  UserException typeError(UnsupportedConversionError e) {
    return UserException
        .dataReadError()
        .message(e.getMessage())
        .addContext("I/O error reading JSON")
        .addContext("Location", parser == null ? options.context : tokenizer.context())
        .build(logger);
  }

  UserException typeError(UnsupportedConversionError e, String msg) {
    return UserException
        .dataReadError()
        .message(msg)
        .addContext("Details", e.getMessage())
        .addContext("I/O error reading JSON")
        .addContext("Location", parser == null ? options.context : tokenizer.context())
        .build(logger);
  }

  @Override
  public void close() {
    if (errorRecoveryCount > 0) {
      logger.warn("Read JSON input {} with {} recoverable error(s).",
          options.context, errorRecoveryCount);
    }
    try {
      parser.close();
    } catch (IOException e) {
      logger.warn("Ignored failure when closing JSON source " + options.context, e);
    }
  }
}
