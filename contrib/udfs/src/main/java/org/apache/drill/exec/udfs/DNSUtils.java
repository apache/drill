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
import org.xbill.DNS.SimpleResolver;
import org.xbill.DNS.TextParseException;
import org.xbill.DNS.Type;

import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

public class DNSUtils {

  /**
   *  A list of known DNS resolvers.
   */
  private static final Map<String, String> KNOWN_RESOLVERS = new HashMap<>();
  static {
    KNOWN_RESOLVERS.put("cloudflare", "1.1.1.1");
    KNOWN_RESOLVERS.put("cloudflare_secondary", "1.0.0.1");
    KNOWN_RESOLVERS.put("google", "8.8.8.8");
    KNOWN_RESOLVERS.put("google_secondary", "8.8.4.4");
    KNOWN_RESOLVERS.put("verisign", "64.6.64.6");
    KNOWN_RESOLVERS.put("verisign_secondary", "64.6.65.6");
    KNOWN_RESOLVERS.put("yandex", "77.88.8.8");
    KNOWN_RESOLVERS.put("yandex_secondary", "77.88.8.1");
  }

  private static final Logger logger = LoggerFactory.getLogger(DNSUtils.class);

  /**
   * Performs the actual DNS lookup and returns the results in a {@link ComplexWriter}.  If the resolver
   * is not null, we will use the provided resolver.  If a resolver is not provided, we'll use the local cache.
   * <p>
   *   Relating to the resolver, you can specify an IP or host, or you can use a name of a known resolver.  Known resolvers are:
   *   <ul>
   *     <li>cloudflare</li>
   *     <li>cloudflare_secondary</li>
   *     <li>google</li>
   *     <li>google_secondary</li>
   *     <li>verisign</li>
   *     <li>verisign_secondary</li>
   *     <li>yandex</li>
   *     <li>yandex_secondary</li>
   *   </ul>
   *
   * </p>
   * @param domainName A {@link String} of a domain for which you want to look up.
   * @param resolverName A {@link String} containing the resolver name.
   * @param out The {@link ComplexWriter} to which the DNS results will be written.
   * @param buffer The {@link DrillBuf} to which the data will be written.
   * @throws TextParseException
   */
  public static void getDNS(String domainName, String resolverName, ComplexWriter out, DrillBuf buffer) throws TextParseException {

    Lookup look = new Lookup(domainName, Type.ANY);
    String resolverIP;
    // Add the resolver if it is provided.
    if (StringUtils.isNotEmpty(resolverName)) {
      // Create a resolver
      if (KNOWN_RESOLVERS.containsKey(resolverName.toLowerCase())) {
        resolverIP = KNOWN_RESOLVERS.get(resolverName.toLowerCase());
      } else {
        resolverIP = resolverName;
      }
      try {
        SimpleResolver resolver = new SimpleResolver(resolverIP);
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
