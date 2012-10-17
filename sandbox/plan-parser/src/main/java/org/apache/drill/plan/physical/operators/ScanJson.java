/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.drill.plan.physical.operators;

import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import com.google.common.io.Files;
import com.google.common.io.InputSupplier;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonStreamParser;
import org.apache.drill.plan.ast.Arg;
import org.apache.drill.plan.ast.Op;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.List;
import java.util.Map;

/**
 * Reads JSON formatted records from a file.
 */
public class ScanJson extends Operator {

    public static void define() {
        Operator.defineOperator("scan-json", ScanJson.class);
    }

    private InputSupplier<InputStreamReader> input;

    public ScanJson(Op op, Map<Integer, Operator> bindings) {
        List<Arg> in = op.getInputs();
        if (in.size() != 1) {
            throw new IllegalArgumentException("scan-json should have exactly one argument (a file name)");
        }
        input = Files.newReaderSupplier(new File(in.get(0).asString()), Charsets.UTF_8);

        List<Arg> out = op.getOutputs();
        if (out.size() != 1) {
            throw new IllegalArgumentException("scan-json should have exactly one output");
        }
        bindings.put(out.get(0).asSymbol().getInt(), this);
    }

    public ScanJson(InputSupplier<InputStreamReader> input) throws IOException {
        this.input = input;
    }

    public static ScanJson create(InputSupplier<InputStreamReader> input) throws IOException {
        return new ScanJson(input);
    }

    @Override
    public void link(Op next, Map<Integer, Operator> bindings) {
        // nothing to look for
    }

    @Override
    public Object call() throws IOException {
        int count = 0;
        Reader in = input.getInput();
        JsonStreamParser jp = new JsonStreamParser(in);
        while (jp.hasNext()) {
            JsonElement r = jp.next();
            emit(r);
            count++;
        }
        in.close();
        return count;
    }

    @Override
    public Schema getSchema() {
        return new JsonSchema();
    }

    private class JsonSchema extends Schema {
        Splitter onDot = Splitter.on(".");

        @Override
        public Object get(String name, Object data) {
            Iterable<String> bits = onDot.split(name);
            for (String bit : bits) {
                data = ((JsonObject) data).get(bit);
            }
            if (data instanceof JsonPrimitive) {
                JsonPrimitive v = (JsonPrimitive) data;
                if (v.isNumber()) {
                    return v.getAsDouble();
                } else if (v.isString()) {
                    return v.getAsString();
                } else {
                    return v.getAsBoolean();
                }
            } else {
                return data;
            }
        }
    }
}
