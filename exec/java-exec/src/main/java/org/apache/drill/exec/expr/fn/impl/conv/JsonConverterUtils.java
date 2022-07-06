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


import org.apache.drill.exec.physical.resultSet.ResultSetLoader;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.store.easy.json.loader.JsonLoaderImpl;
import org.apache.drill.exec.store.easy.json.loader.JsonLoaderImpl.JsonLoaderBuilder;
import org.apache.drill.exec.store.easy.json.loader.SingleElementIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;

public class JsonConverterUtils {

  private static final Logger logger = LoggerFactory.getLogger(JsonConverterUtils.class);

  /*
  public static InputStream convertStringToInputStream(String input) {
    try (InputStream stream = IOUtils.toInputStream(input, Charset.defaultCharset())) {
      return stream;
    } catch (IOException e) {
      throw UserException.dataReadError(e)
        .message("Unable to read JSON string")
        .build(logger);
    }
  }
  */

  /**
   * Creates a {@link JsonLoaderImpl} for use in JSON conversion UDFs.
   * @param rsLoader The {@link ResultSetLoader} used in the UDF
   * @param options The {@link OptionManager} used in the UDF.  This is used to extract the global JSON options
   * @param stream An input stream containing the input JSON data
   * @return A {@link JsonLoaderImpl} for use in the UDF.
   */
  public static JsonLoaderImpl createJsonLoader(ResultSetLoader rsLoader,
                                                OptionManager options,
                                                SingleElementIterator<InputStream> stream) {
    // Add JSON configuration from Storage plugin, if present.
    JsonLoaderBuilder jsonLoaderBuilder = new JsonLoaderBuilder()
        .resultSetLoader(rsLoader)
        .standardOptions(options)
        .fromStream(() -> stream);

    return (JsonLoaderImpl) jsonLoaderBuilder.build();
  }

}
