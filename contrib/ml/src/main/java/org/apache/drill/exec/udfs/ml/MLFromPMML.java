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
package org.apache.drill.exec.udfs.ml;

import io.netty.buffer.DrillBuf;
import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.annotations.Workspace;
import org.apache.drill.exec.expr.holders.VarCharHolder;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.vector.complex.reader.FieldReader;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.ComplexWriter;

import javax.inject.Inject;

/**
 * {@code predict(model_location, feature...)} scores a row with a PMML model stored in a
 * file system workspace and returns a map of the predicted value and any probabilities.
 */
@SuppressWarnings("unused")
public class MLFromPMML {

  @FunctionTemplate(names = {"predict"},
    scope = FunctionTemplate.FunctionScope.SIMPLE,
    isVarArg = true)
  public static class PredictionFunction implements DrillSimpleFunc {

    @Param(constant = true)
    VarCharHolder rawPath;

    @Param
    FieldReader[] features;

    @Output
    ComplexWriter outWriter;

    @Inject
    DrillBuf outBuffer;

    @Inject
    DrillbitContext drillbitContext;

    @Workspace
    org.pmml4s.model.Model model;

    @Override
    public void setup() {
    }

    @Override
    public void eval() {
      if (model == null) {
        String location = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers
          .toStringFromUTF8(rawPath.start, rawPath.end, rawPath.buffer);
        model = org.apache.drill.exec.udfs.ml.MLUtils.getModel(location, drillbitContext);
      }
      java.util.Map predictions = org.apache.drill.exec.udfs.ml.MLUtils.predict(model, features);
      outBuffer = org.apache.drill.exec.udfs.ml.MLUtils.mapFields(predictions, outWriter, outBuffer);
    }
  }
}
