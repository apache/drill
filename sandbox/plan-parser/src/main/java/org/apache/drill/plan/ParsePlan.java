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

package org.apache.drill.plan;

import com.google.common.base.Charsets;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import com.google.common.io.Files;
import com.google.common.io.InputSupplier;
import com.google.common.io.Resources;
import org.antlr.runtime.ANTLRReaderStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;
import org.apache.drill.plan.ast.*;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Formatter;

/**
 * Parses a plan from a resource or file.
 *
 * The result is validated to ensure that symbols mentioned on the left-hand side of assignments are only mentioned
 * once and all referenced symbols on the right hand side are defined somewhere.
 */
public class ParsePlan {
    public static Plan parseResource(File file) throws IOException, RecognitionException, ValidationException {
        return ParsePlan.parse(Files.newReaderSupplier(file, Charsets.UTF_8));
    }

    public static Plan parseResource(String resourceName) throws IOException, RecognitionException, ValidationException {
        return ParsePlan.parse(Resources.newReaderSupplier(Resources.getResource(resourceName), Charsets.UTF_8));
    }

    public static Plan parse(InputSupplier<InputStreamReader> in) throws IOException, RecognitionException, ValidationException {
        InputStreamReader inStream = in.getInput();
        PlanLexer lex = new PlanLexer(new ANTLRReaderStream(inStream));
        PlanParser r = new PlanParser(new CommonTokenStream(lex));
        inStream.close();

        Plan plan = r.plan().r;
        validate(plan);
        return plan;
    }

    private static void validate(Plan r) throws ValidationException {
        int errors = 0;
        Formatter errorMessages = new Formatter();

        // make sure that each output is assigned only once
        Multiset<Integer> counts = HashMultiset.create();
        int line = 1;
        for (Op op : r.getStatements()) {
            for (Arg assignment : op.getOutputs()) {
                int slot = ((Arg.Symbol) assignment).getSlot();
                counts.add(slot);
                if (counts.count(slot) != 1) {
                    errorMessages.format("Output symbol %%%d used more than once in statement %d\n", slot, line);
                    errors++;
                }
            }
            line++;
        }

        // make sure that each input is defined at least once
        line = 1;
        for (Op op : r.getStatements()) {
            for (Arg reference : op.getInputs()) {
                if (reference instanceof Arg.Symbol) {
                    int slot = ((Arg.Symbol) reference).getSlot();
                    if (counts.count(slot) <= 0) {
                        errorMessages.format("Undefined reference to %%%d in statement %d\n", slot, line);
                        errors++;
                    }
                }
            }
            line++;
        }

        if (errors > 0) {
            throw new ValidationException(errorMessages.toString());
        }
    }

    public static class ValidationException extends Exception {
        public ValidationException(String s) {
            super(s);
        }
    }
}
