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

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.drill.common.exceptions.CustomErrorContext;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.physical.resultSet.ResultSetLoader;
import org.apache.drill.exec.physical.resultSet.RowSetLoader;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.store.easy.json.parser.ErrorFactory;
import org.apache.drill.exec.store.easy.json.parser.JsonStructureParser;
import org.apache.drill.exec.store.easy.json.parser.ValueDef;
import org.apache.drill.exec.store.easy.json.parser.ValueDef.JsonType;
import org.apache.drill.exec.vector.accessor.UnsupportedConversionError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esri.core.geometry.JsonReader;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonToken;

/**
 * Revised JSON loader that is based on the
 * {@link ResultSetLoader} abstraction. Uses the listener-based
 * {@link JsonStructureParser} to walk the JSON tree in a "streaming"
 * fashion, calling events which this class turns into vector write
 * operations. Listeners handle options such as all text mode
 * vs. type-specific parsing. Think of this implementation as a
 * listener-based recursive-descent parser.
 * <p>
 * The JSON loader mechanism runs two state machines intertwined:
 * <ol>
 * <li>The actual parser (to parse each JSON object, array or scalar according
 * to its inferred type represented by the {@code JsonStructureParser}.</li>
 * <li>The type discovery machine, which is made complex because JSON may include
 * long runs of nulls, represented by this class.</li>
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
 * <h4>Provided Schema</h4>
 *
 * The JSON loader accepts a provided schema which removes type ambiguities.
 * If we have the examples above (runs of nulls, or shifting types), then the
 * provided schema says the vector type to create; the individual column listeners
 * attempt to convert the JSON token type to the target vector type. The result
 * is that, if the schema provides the correct type, the loader can ride over
 * ambiguities in the input.
 *
 * <h4>Comparison to Original JSON Reader</h4>
 *
 * This class replaces the {@link JsonReader} class used in Drill versions 1.17
 * and before. Compared with the previous version, this implementation:
 * <ul>
 * <li>Materializes parse states as classes rather than as methods and
 * boolean flags as in the prior version.</li>
 * <li>Reports errors as {@link UserException} objects, complete with context
 * information, rather than as generic Java exception as in the prior version.</li>
 * <li>Moves parse options into a separate {@link JsonOptions} class.</li>
 * <li>Iteration protocol is simpler: simply call {@link #next()} until it returns
 * {@code false}. Errors are reported out-of-band via an exception.</li>
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
 * projected for {@code SELECT COUNT(*)} queries.</li>
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
public class JsonLoaderImpl implements JsonLoader, ErrorFactory {
  protected static final Logger logger = LoggerFactory.getLogger(JsonLoaderImpl.class);

  interface NullTypeMarker {
    void forceResolution();
  }

  private final ResultSetLoader rsLoader;
  private final JsonLoaderOptions options;
  private final CustomErrorContext errorContext;
  private final TupleListener rowListener;
  private final JsonStructureParser parser;
  private boolean eof;

  /**
   * List of "unknown" columns (have only seen nulls or empty values)
   * that are waiting for resolution, or forced resolution at the end
   * of a batch. Unknown columns occur only when using dynamic type
   * inference, and not JSON tokens have been seen which would hint
   * at a type. Not needed when a schema is provided.
   */

  // Using a simple list. Won't perform well if we have hundreds of
  // null fields; but then we've never seen such a pathologically bad
  // case... Usually just one or two fields have deferred nulls.
  private final List<NullTypeMarker> nullStates = new ArrayList<>();

  public JsonLoaderImpl(ResultSetLoader rsLoader, TupleMetadata providedSchema,
      JsonLoaderOptions options, CustomErrorContext errorContext,
      InputStream stream) {
    this.rsLoader = rsLoader;
    this.options = options;
    this.errorContext = errorContext;
    this.rowListener = new TupleListener(this, rsLoader.writer(), providedSchema);
    this.parser = new JsonStructureParser(stream, options, rowListener, this);
  }

  public JsonLoaderOptions options() { return options; }

  @Override // JsonLoader
  public boolean next() {
    if (eof) {
      return false;
    }
    rsLoader.startBatch();
    RowSetLoader rowWriter = rsLoader.writer();
    while (rowWriter.start()) {
      if (parser.next()) {
        rowWriter.save();
      } else {
        eof = true;
        break;
      }
    }
    return rsLoader.hasRows();
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
   * we do the following guess Varchar, and switch to text mode.
   * <p>
   * This choices is not perfect. Switching to text mode means
   * results will vary
   * from run to run depending on the order that we see empty and
   * non-empty values for this column. Plus, since the system is
   * distributed, the decision made here may conflict with that made in
   * some other fragment.
   * <p>
   * The only real solution is for the user to provide a schema.
   * <p>
   * Bottom line: the user is responsible for not giving Drill
   * ambiguous data that would require Drill to predict the future.
   */
  @Override // JsonLoader
  public void endBatch() {

    // Make a copy. Forcing resolution will remove the
    // element from the original list.
    List<NullTypeMarker> copy = new ArrayList<>();
    copy.addAll(nullStates);
    for (NullTypeMarker marker : copy) {
      marker.forceResolution();
    }
    assert nullStates.isEmpty();
  }

  @Override // JsonLoader
  public void close() {
    parser.close();
  }

  @Override // ErrorFactory
  public RuntimeException parseError(String msg, JsonParseException e) {
    throw buildError(
        UserException.dataReadError(e)
          .addContext(msg));
  }

  @Override // ErrorFactory
  public RuntimeException ioException(IOException e) {
    throw buildError(
        UserException.dataReadError(e)
          .addContext(errorContext));
  }

  @Override // ErrorFactory
  public RuntimeException structureError(String msg) {
    throw buildError(
        UserException.dataReadError()
          .message(msg));
  }

  @Override // ErrorFactory
  public RuntimeException syntaxError(JsonParseException e) {
    throw buildError(
        UserException.dataReadError(e)
          .addContext("Syntax error"));
  }

  @Override // ErrorFactory
  public RuntimeException typeError(UnsupportedConversionError e) {
    throw buildError(
        UserException.validationError(e)
          .addContext("Unsupported type conversion"));
  }

  @Override // ErrorFactory
  public RuntimeException syntaxError(JsonToken token) {
    throw buildError(
        UserException.dataReadError()
          .addContext("Syntax error on token", token.toString()));
  }

  @Override // ErrorFactory
  public RuntimeException unrecoverableError() {
    throw buildError(
        UserException.dataReadError()
          .addContext("Unrecoverable syntax error on token")
          .addContext("Recovery attempts", parser.recoverableErrorCount()));
  }

  protected UserException typeConversionError(ColumnMetadata schema, ValueDef valueDef) {
    StringBuilder buf = new StringBuilder()
        .append(valueDef.type().name().toLowerCase());
    if (valueDef.isArray()) {
      for (int i = 0; i < valueDef.dimensions(); i++) {
        buf.append("[]");
      }
    }
    return typeConversionError(schema, buf.toString());
  }

  protected UserException typeConversionError(ColumnMetadata schema, String tokenType) {
    return buildError(schema,
        UserException.dataReadError()
          .message("Type of JSON token is not compatible with its column")
          .addContext("JSON token type", tokenType));
  }

  protected UserException dataConversionError(ColumnMetadata schema, String tokenType, String value) {
    return buildError(schema,
        UserException.dataReadError()
          .message("Type of JSON token is not compatible with its column")
          .addContext("JSON token type", tokenType)
          .addContext("JSON token", value));
  }

  public UserException unsupportedType(ColumnMetadata schema) {
    return buildError(schema,
        UserException.validationError()
          .message("JSON reader does not support the provided column type"));
  }

  public UserException unsupportedJsonTypeException(String key, JsonType jsonType) {
    return buildError(
        UserException.dataReadError()
          .message("JSON reader does not support the JSON data type")
          .addContext("Field", key)
          .addContext("JSON type", jsonType.toString()));
  }

  public UserException unsupportedArrayException(String key, int dims) {
    return buildError(
        UserException.validationError()
          .message("JSON reader does not arrays deeper than two levels")
          .addContext("Field", key)
          .addContext("Array nesting", dims));
  }

  protected UserException buildError(ColumnMetadata schema, UserException.Builder builder) {
    return buildError(builder
        .addContext("Column", schema.name())
        .addContext("Column type", schema.typeString()));
  }

  protected UserException buildError(UserException.Builder builder) {
    builder
      .addContext(errorContext)
      .addContext("Line", parser.lineNumber())
      .addContext("Position", parser.columnNumber());
    String token = parser.token();
    if (token != null) {
      builder.addContext("Near token", token);
    }
    return builder.build(logger);
  }
}
