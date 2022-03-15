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

package org.apache.drill.exec.udfs.http;

import io.netty.buffer.DrillBuf;
import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.annotations.Workspace;
import org.apache.drill.exec.vector.complex.reader.FieldReader;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.ComplexWriter;

import javax.inject.Inject;

public class HttpHelperFunctions {

  @FunctionTemplate(names = {"http_get", "httpGet"},
    scope = FunctionTemplate.FunctionScope.SIMPLE,
    isVarArg = true)
  public static class HttpGetFunction implements DrillSimpleFunc {

    @Param
    FieldReader[] inputReaders;

    @Output
    ComplexWriter writer;

    @Inject
    DrillBuf buffer;

    @Workspace
    org.apache.drill.exec.vector.complex.fn.JsonReader jsonReader;

    @Override
    public void setup() {
      jsonReader = new org.apache.drill.exec.vector.complex.fn.JsonReader.Builder(buffer)
        .defaultSchemaPathColumns()
        .build();
    }

    @Override
    public void eval() {
      if (inputReaders.length > 0) {
        // Get the URL
        FieldReader urlReader = inputReaders[0];
        String url = urlReader.readObject().toString();

        // Process Positional Arguments
        java.util.List args = org.apache.drill.exec.udfs.http.HttpHelperUtils.buildParameterList(inputReaders);
        String finalUrl = org.apache.drill.exec.util.HttpUtils.mapPositionalParameters(url, args);

        // Make the API call
        String results = org.apache.drill.exec.util.HttpUtils.makeSimpleGetRequest(finalUrl);

        // If the result string is null or empty, return an empty map
        if (results == null || results.length() == 0) {
          // Return empty map
          org.apache.drill.exec.vector.complex.writer.BaseWriter.MapWriter mapWriter = writer.rootAsMap();
          mapWriter.start();
          mapWriter.end();
          return;
        }

        try {
          jsonReader.setSource(results);
          jsonReader.setIgnoreJSONParseErrors(true);  // Reduce number of errors
          jsonReader.write(writer);
          buffer = jsonReader.getWorkBuf();
        } catch (Exception e) {
          throw new org.apache.drill.common.exceptions.DrillRuntimeException("Error while converting from JSON. ", e);
        }
      }
    }
  }
}
