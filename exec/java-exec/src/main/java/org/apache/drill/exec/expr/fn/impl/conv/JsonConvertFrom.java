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


import javax.inject.Inject;

import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.FunctionScope;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.NullHandling;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.annotations.Workspace;
import org.apache.drill.exec.expr.holders.NullableVarBinaryHolder;
import org.apache.drill.exec.expr.holders.NullableVarCharHolder;
import org.apache.drill.exec.physical.resultSet.ResultSetLoader;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.vector.complex.writer.BaseWriter;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.ComplexWriter;
@SuppressWarnings("unused")
public class JsonConvertFrom {

  private JsonConvertFrom() {}

  @FunctionTemplate(name = "convert_fromJSON",
    scope = FunctionScope.SIMPLE, nulls = NullHandling.INTERNAL)
  public static class ConvertFromJsonNullableInput implements DrillSimpleFunc {

    @Param
    NullableVarBinaryHolder in;

    @Output // TODO Remove in future work
    BaseWriter.ComplexWriter writer;

    @Inject
    OptionManager options;

    @Inject
    ResultSetLoader rsLoader;

    @Workspace
    org.apache.drill.exec.store.easy.json.loader.SingleElementIterator<java.io.InputStream> streamIter;

    @Workspace
    org.apache.drill.exec.store.easy.json.loader.JsonLoaderImpl jsonLoader;

    @Override
    public void setup() {
      streamIter = new org.apache.drill.exec.store.easy.json.loader.SingleElementIterator<>();
      rsLoader.startBatch();
    }

    @Override
    public void eval() {
      // If the input is null or empty, return an empty map
      if (in.isSet == 0 || in.start == in.end) {
        return;
      }

      java.io.InputStream inputStream = org.apache.drill.exec.vector.complex.fn.DrillBufInputStream.getStream(in.start, in.end, in.buffer);

      try {
        streamIter.setValue(inputStream);

        if (jsonLoader == null) {
          jsonLoader = org.apache.drill.exec.expr.fn.impl.conv.JsonConverterUtils.createJsonLoader(rsLoader, options, streamIter);
        }

        org.apache.drill.exec.physical.resultSet.RowSetLoader rowWriter = rsLoader.writer();
        rowWriter.start();
        if (jsonLoader.parser().next()) {
          rowWriter.save();
        }
        //inputStream.close();

      } catch (Exception e) {
        throw org.apache.drill.common.exceptions.UserException.dataReadError(e)
          .message("Error while reading JSON. ")
          .addContext(e.getMessage())
          .build();
      }
    }
  }

  @FunctionTemplate(name = "convert_fromJSON",
    scope = FunctionScope.SIMPLE, nulls = NullHandling.INTERNAL)
  public static class ConvertFromJsonVarcharInput implements DrillSimpleFunc {

    @Param
    NullableVarCharHolder in;

    @Output // TODO Remove in future work
    ComplexWriter writer;

    @Workspace
    org.apache.drill.exec.store.easy.json.loader.SingleElementIterator<java.io.InputStream> streamIter;

    @Inject
    OptionManager options;

    @Inject
    ResultSetLoader rsLoader;

    @Workspace
    org.apache.drill.exec.store.easy.json.loader.JsonLoaderImpl jsonLoader;

    @Override
    public void setup() {
      streamIter = new org.apache.drill.exec.store.easy.json.loader.SingleElementIterator<>();
      rsLoader.startBatch();
    }

    @Override
    public void eval() {
      // If the input is null or empty, return an empty map
      if (in.isSet == 0 || in.start == in.end) {
        return;
      }

      java.io.InputStream inputStream = org.apache.drill.exec.vector.complex.fn.DrillBufInputStream.getStream(in.start, in.end, in.buffer);

      try {
        streamIter.setValue(inputStream);
        if (jsonLoader == null) {
          jsonLoader = org.apache.drill.exec.expr.fn.impl.conv.JsonConverterUtils.createJsonLoader(rsLoader, options, streamIter);
        }
        org.apache.drill.exec.physical.resultSet.RowSetLoader rowWriter = rsLoader.writer();
        rowWriter.start();
        if (jsonLoader.parser().next()) {
          rowWriter.save();
        }
      } catch (Exception e) {
        throw org.apache.drill.common.exceptions.UserException.dataReadError(e)
          .message("Error while reading JSON. ")
          .addContext(e.getMessage())
          .build();
      }
    }
  }
}
