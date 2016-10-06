package org.apache.drill.exec.expr.fn.impl;

//*

import io.netty.buffer.DrillBuf;
import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.holders.NullableVarCharHolder;
import org.apache.drill.exec.vector.complex.writer.BaseWriter;

import javax.inject.Inject;

/* Copyright 2001-2004 The Apache Software Foundation.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*      http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

@FunctionTemplate(
        name="parse_query",
        scope= FunctionTemplate.FunctionScope.SIMPLE,
        nulls = FunctionTemplate.NullHandling.NULL_IF_NULL
)

public class ParseQueryFunction implements DrillSimpleFunc {

    @Param
    NullableVarCharHolder input;

    @Output
    BaseWriter.ComplexWriter outWriter;

    @Inject
    DrillBuf outBuffer;

    public void setup() {
    }

    public void eval() {

        org.apache.drill.exec.vector.complex.writer.BaseWriter.MapWriter queryMapWriter = outWriter.rootAsMap();

        String queryString = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(input.start, input.end, input.buffer);

        if( queryString.isEmpty() || queryString.equals("null")){
            queryString = "";
        }

        String firstLetter = queryString.substring(0, 1);

        //If the first character is a &, it doesn't split properly.  This checks to see if the first character is an & and if so, removes it.
        if(firstLetter.equals("&")){
            queryString = queryString.substring(1);
        }

        String[] arguments = queryString.split("&");

        for (int i = 0; i < arguments.length; i++) {
            String[] queryParts = arguments[i].split("=");

            org.apache.drill.exec.expr.holders.VarCharHolder rowHolder = new org.apache.drill.exec.expr.holders.VarCharHolder();

            byte[] rowStringBytes = queryParts[1].getBytes();

            outBuffer.reallocIfNeeded(rowStringBytes.length);
            outBuffer.setBytes(0, rowStringBytes);

            rowHolder.start = 0;
            rowHolder.end = rowStringBytes.length;
            rowHolder.buffer = outBuffer;

            queryMapWriter.varChar(queryParts[0]).write(rowHolder);

        }
    }
}