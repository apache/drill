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

package org.apache.drill.exec.udfs;

import io.netty.buffer.DrillBuf;
import org.apache.commons.lang3.StringUtils;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.expr.holders.VarCharHolder;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.ComplexWriter;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.ListWriter;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.MapWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xbill.DNS.Lookup;
import org.xbill.DNS.Record;
import org.xbill.DNS.Resolver;
import org.xbill.DNS.SimpleResolver;
import org.xbill.DNS.TextParseException;
import org.xbill.DNS.Type;

import java.net.UnknownHostException;

public class DNSUtils {

  private static final Logger logger = LoggerFactory.getLogger(DNSUtils.class);
  public static void getDNS(String domainName, String resolverName, ComplexWriter out, DrillBuf buffer) throws TextParseException {

    Lookup look = new Lookup(domainName, Type.ANY);

    // Add the resolver if it is provided.
    if (StringUtils.isNotEmpty(resolverName)) {
      // Create a resolver
      try {
        SimpleResolver resolver = new SimpleResolver(resolverName);
        look.setResolver(resolver);
      } catch (UnknownHostException e) {
        throw UserException.connectionError(e)
            .message("Cannot find resolver: " + resolverName)
            .build(logger);
      }
    }


    Record[] records = look.run();

    // Initialize writers
    ListWriter listWriter = out.rootAsList();
    MapWriter rowMapWriter = listWriter.map();
    // If there are no records, return an empty list.
    if (records == null) {
      listWriter.startList();
      rowMapWriter.start();
      rowMapWriter.end();
      listWriter.endList();
      return;
    }

    for (int i = 0; i < records.length; i++) {
      VarCharHolder fieldHolder = new VarCharHolder();

      Record record = records[i];

      rowMapWriter.start();
      byte[] name = record.getName().toString().getBytes();
      buffer = buffer.reallocIfNeeded(name.length);
      buffer.setBytes(0, name);
      rowMapWriter.varChar("name").writeVarChar(0, name.length, buffer);

      byte[] completeRecord = record.toString().getBytes();
      buffer = buffer.reallocIfNeeded(completeRecord.length);
      buffer.setBytes(0, completeRecord);
      fieldHolder.start = 0;
      fieldHolder.end = completeRecord.length;
      fieldHolder.buffer = buffer;
      rowMapWriter.varChar("record").write(fieldHolder);

      rowMapWriter.bigInt("ttl").writeBigInt(record.getTTL());

      byte[] type = Type.string(record.getType()).getBytes();
      buffer = buffer.reallocIfNeeded(type.length);
      buffer.setBytes(0, type);
      rowMapWriter.varChar("type").writeVarChar(0, type.length, buffer);

      byte[] rdata = record.rdataToString().getBytes();
      buffer.reallocIfNeeded(rdata.length);
      buffer.setBytes(0, rdata);
      rowMapWriter.varChar("rdata").writeVarChar(0, rdata.length, buffer);
      rowMapWriter.end();
    }
  }

  public static void getDNS(String domainName, ComplexWriter out, DrillBuf buffer) throws TextParseException {
    getDNS(domainName, null, out, buffer);
  }
}
