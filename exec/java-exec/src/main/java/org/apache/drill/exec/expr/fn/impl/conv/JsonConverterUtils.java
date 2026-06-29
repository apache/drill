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

package org.apache.drill.exec.expr.fn.impl.conv;


import io.netty.buffer.DrillBuf;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.physical.resultSet.ResultSetLoader;
import org.apache.drill.exec.physical.resultSet.RowSetLoader;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.store.easy.json.loader.ClosingStreamIterator;
import org.apache.drill.exec.store.easy.json.loader.JsonLoaderImpl;
import org.apache.drill.exec.store.easy.json.loader.JsonLoaderImpl.JsonLoaderBuilder;
import org.apache.drill.exec.store.easy.json.loader.JsonLoaderOptions;
import org.apache.drill.exec.vector.complex.fn.DrillBufInputStream;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonConverterUtils {

  private static final Logger logger = LoggerFactory.getLogger(JsonConverterUtils.class);

  /**
   * Field name used to wrap the input value so that the record-oriented JSON
   * loader can read top-level scalars and arrays (not just objects) uniformly.
   * The single resulting column carries the converted value and is recognised
   * and unwrapped by {@code ProjectRecordBatch} (which transfers it directly to
   * the output column instead of wrapping the loader's columns in a map).
   */
  public static final String WRAP_FIELD = "drill_json_value_wrapper";

  private static final byte[] WRAP_PREFIX =
      ("{\"" + WRAP_FIELD + "\":").getBytes(StandardCharsets.UTF_8);
  private static final byte[] WRAP_SUFFIX = "}".getBytes(StandardCharsets.UTF_8);

  private JsonConverterUtils() {
  }

  /**
   * Creates a {@link JsonLoaderImpl} for use in JSON conversion UDFs, using the
   * system JSON options from the {@link OptionManager}.
   *
   * @param rsLoader The {@link ResultSetLoader} used in the UDF
   * @param options The {@link OptionManager} used in the UDF.  This is used to extract the global JSON options
   * @param stream An input stream containing the input JSON data
   * @return A {@link JsonLoaderImpl} for use in the UDF.
   */
  public static JsonLoaderImpl createJsonLoader(ResultSetLoader rsLoader,
                                                OptionManager options,
                                                ClosingStreamIterator stream) {
    JsonLoaderBuilder jsonLoaderBuilder = new JsonLoaderBuilder()
        .resultSetLoader(rsLoader)
        .standardOptions(options)
        .fromStream(() -> stream);

    return (JsonLoaderImpl) jsonLoaderBuilder.build();
  }

  /**
   * Creates a {@link JsonLoaderImpl} for use in JSON conversion UDFs, overriding the
   * {@code allTextMode} and {@code readNumbersAsDouble} options with the values supplied
   * as function arguments.  Remaining options are taken from the system JSON options.
   *
   * @param rsLoader The {@link ResultSetLoader} used in the UDF
   * @param options The {@link OptionManager} used in the UDF.  This is used to extract the global JSON options
   * @param stream An input stream containing the input JSON data
   * @param allTextMode Whether to read all scalars as text
   * @param readNumbersAsDouble Whether to read all numbers as doubles
   * @return A {@link JsonLoaderImpl} for use in the UDF.
   */
  public static JsonLoaderImpl createJsonLoader(ResultSetLoader rsLoader,
                                                OptionManager options,
                                                ClosingStreamIterator stream,
                                                boolean allTextMode,
                                                boolean readNumbersAsDouble) {
    JsonLoaderOptions jsonOptions = new JsonLoaderOptions(options);
    jsonOptions.allTextMode = allTextMode;
    jsonOptions.readNumbersAsDouble = readNumbersAsDouble;

    JsonLoaderBuilder jsonLoaderBuilder = new JsonLoaderBuilder()
        .resultSetLoader(rsLoader)
        .options(jsonOptions)
        .fromStream(() -> stream);

    return (JsonLoaderImpl) jsonLoaderBuilder.build();
  }

  /**
   * Converts a single JSON value (one row of UDF input) into the result set loader.
   *
   * <p>Exactly one row is always written to the loader -- even for null/empty
   * input -- so that the loader's row count stays aligned with the surrounding
   * project batch. The value is wrapped in a single-field object so that
   * top-level scalars and arrays parse the same way as objects.</p>
   *
   * @param rsLoader the result set loader injected into the UDF
   * @param jsonLoader the lazily created JSON loader, or {@code null} on the first call
   * @param options the system JSON options
   * @param stream the streaming iterator bound to the JSON loader
   * @param isSet 0 if the input value is null, 1 otherwise
   * @param start start offset of the value in {@code buffer}
   * @param end end offset of the value in {@code buffer}
   * @param buffer buffer holding the input value
   * @param useArgs whether to honour the explicit allTextMode/readNumbersAsDouble arguments
   * @param allTextMode explicit all-text-mode argument (used only when {@code useArgs})
   * @param readNumbersAsDouble explicit read-numbers-as-double argument (used only when {@code useArgs})
   * @return the JSON loader (created on first use), to be cached by the caller
   */
  public static JsonLoaderImpl convertJson(ResultSetLoader rsLoader,
                                           JsonLoaderImpl jsonLoader,
                                           OptionManager options,
                                           ClosingStreamIterator stream,
                                           int isSet, int start, int end, DrillBuf buffer,
                                           boolean useArgs,
                                           boolean allTextMode,
                                           boolean readNumbersAsDouble) {
    RowSetLoader rowWriter = rsLoader.writer();
    rowWriter.start();
    // For null or empty input emit an (unset) row to keep the row count aligned.
    if (isSet == 0 || start == end) {
      rowWriter.save();
      return jsonLoader;
    }

    try {
      stream.setValue(getWrappedJsonStream(start, end, buffer));
      if (jsonLoader == null) {
        jsonLoader = useArgs
            ? createJsonLoader(rsLoader, options, stream, allTextMode, readNumbersAsDouble)
            : createJsonLoader(rsLoader, options, stream);
      }
      // next() reads the single wrapped record; always save the row so the
      // count stays aligned even for an empty document.
      jsonLoader.parser().next();
      rowWriter.save();
    } catch (Exception e) {
      throw UserException.dataReadError(e)
          .message("Error while reading JSON. ")
          .addContext(e.getMessage())
          .build(logger);
    }
    return jsonLoader;
  }

  /**
   * Wraps the raw input value in a single-field JSON object ({@code {"json": <value>}})
   * so that the record-oriented JSON loader accepts top-level scalars and arrays.
   */
  private static InputStream getWrappedJsonStream(int start, int end, DrillBuf buffer) {
    InputStream value = DrillBufInputStream.getStream(start, end, buffer);
    return new SequenceInputStream(Collections.enumeration(Arrays.asList(
        new ByteArrayInputStream(WRAP_PREFIX),
        value,
        new ByteArrayInputStream(WRAP_SUFFIX))));
  }
}
