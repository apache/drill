package org.apache.drill.exec.expr.fn.impl;

/*
 * Copyright 2001-2004 The Apache Software Foundation.
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
import io.netty.buffer.DrillBuf;
import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.holders.VarCharHolder;
import org.apache.drill.exec.vector.complex.writer.BaseWriter;

import javax.inject.Inject;

@FunctionTemplate(
        name="parse_url",
        scope= FunctionTemplate.FunctionScope.SIMPLE,
        nulls = FunctionTemplate.NullHandling.NULL_IF_NULL
)

public class ParseUrlFunction implements DrillSimpleFunc {

    @Param VarCharHolder input;

    @Output BaseWriter.ComplexWriter outWriter;

    @Inject DrillBuf outBuffer;

    public void setup() {}

    public void eval() {

        org.apache.drill.exec.vector.complex.writer.BaseWriter.MapWriter urlMapWriter = outWriter.rootAsMap();

        String urlString = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(input.start, input.end, input.buffer);

        try {
            java.net.URL aURL = new java.net.URL(urlString);

            String protocol = aURL.getProtocol();
            String authority = aURL.getAuthority();
            String host = aURL.getHost();
            java.lang.Integer port = aURL.getPort();
            String path = aURL.getPath();
            String query = aURL.getQuery();
            String filename = aURL.getFile();
            String ref = aURL.getRef();

            org.apache.drill.exec.expr.holders.VarCharHolder rowHolder = new org.apache.drill.exec.expr.holders.VarCharHolder();

            byte[] rowStringBytes = protocol.getBytes();

            outBuffer.reallocIfNeeded(rowStringBytes.length);
            outBuffer.setBytes(0, rowStringBytes);

            rowHolder.start = 0;
            rowHolder.end = rowStringBytes.length;
            rowHolder.buffer = outBuffer;

            urlMapWriter.varChar("protocol").write(rowHolder);


            byte[] authRowStringBytes = authority.getBytes();

            outBuffer.reallocIfNeeded(authRowStringBytes.length);
            outBuffer.setBytes(0, authRowStringBytes);

            rowHolder.start = 0;
            rowHolder.end = authRowStringBytes.length;
            rowHolder.buffer = outBuffer;

            urlMapWriter.varChar("authority").write(rowHolder);


            byte[] hostRowStringBytes = host.getBytes();

            outBuffer.reallocIfNeeded(hostRowStringBytes.length);
            outBuffer.setBytes(0, hostRowStringBytes);

            rowHolder.start = 0;
            rowHolder.end = hostRowStringBytes.length;
            rowHolder.buffer = outBuffer;

            urlMapWriter.varChar("host").write(rowHolder);


            byte[] pathRowStringBytes = path.getBytes();

            outBuffer.reallocIfNeeded(pathRowStringBytes.length);
            outBuffer.setBytes(0, pathRowStringBytes);

            rowHolder.start = 0;
            rowHolder.end = pathRowStringBytes.length;
            rowHolder.buffer = outBuffer;

            urlMapWriter.varChar("path").write(rowHolder);


            byte[] queryRowStringBytes = query.getBytes();

            outBuffer.reallocIfNeeded(queryRowStringBytes.length);
            outBuffer.setBytes(0, queryRowStringBytes);

            rowHolder.start = 0;
            rowHolder.end = queryRowStringBytes.length;
            rowHolder.buffer = outBuffer;

            urlMapWriter.varChar("query").write(rowHolder);


            byte[] filenameRowStringBytes = filename.getBytes();

            outBuffer.reallocIfNeeded(filenameRowStringBytes.length);
            outBuffer.setBytes(0, filenameRowStringBytes);

            rowHolder.start = 0;
            rowHolder.end = filenameRowStringBytes.length;
            rowHolder.buffer = outBuffer;

            urlMapWriter.varChar("filename").write(rowHolder);


            byte[] refRowStringBytes = ref.getBytes();

            outBuffer.reallocIfNeeded(refRowStringBytes.length);
            outBuffer.setBytes(0, refRowStringBytes);

            rowHolder.start = 0;
            rowHolder.end = refRowStringBytes.length;
            rowHolder.buffer = outBuffer;

            urlMapWriter.varChar("ref").write(rowHolder);

            org.apache.drill.exec.expr.holders.IntHolder intHolder = new org.apache.drill.exec.expr.holders.IntHolder();
            intHolder.value = port;
            urlMapWriter.integer("port").write(intHolder);
        }
        catch (Exception e ) {}
    }
}
