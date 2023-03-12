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
import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.annotations.Workspace;
import org.apache.drill.exec.expr.holders.NullableVarCharHolder;
import org.apache.drill.exec.expr.holders.VarCharHolder;
import org.apache.drill.exec.vector.complex.writer.BaseWriter;

import javax.inject.Inject;

public class DNSFunctions {

  private DNSFunctions() {
  }

  /* This function gets the host name associated with an IP address */
  @FunctionTemplate(names = {"get_host_name", "reverse_ip_lookup"}, scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)

  public static class ReverseIPLookup implements DrillSimpleFunc {

    @Param
    VarCharHolder ipaddress;

    @Output
    VarCharHolder out;

    @Inject
    DrillBuf buffer;

    @Override
    public void setup() {

    }

    @Override
    public void eval() {
      String ipString = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(ipaddress.start, ipaddress.end, ipaddress.buffer);
      String hostname = "";

      try {
        java.net.InetAddress address = java.net.InetAddress.getByName(ipString);
        hostname = address.getHostName();

      } catch (java.net.UnknownHostException e) {
        hostname = "Unknown host";
      }
      out.buffer = buffer;
      out.start = 0;
      out.end = hostname.getBytes().length;
      buffer.setBytes(0, hostname.getBytes());
    }
  }

  /* This function takes a host name and returns the IP address associated with that host, and "Unknown if there is an error */
  @FunctionTemplate(names = {"get_host_address", "host_lookup"}, scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)

  public static class HostLookup implements DrillSimpleFunc {

    @Param
    VarCharHolder hostname;

    @Output
    VarCharHolder out;

    @Inject
    DrillBuf buffer;

    @Override
    public void setup() {

    }

    @Override
    public void eval() {
      String host = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(hostname.start, hostname.end, hostname.buffer);
      String result = "";
      try {
        java.net.Inet4Address ip = (java.net.Inet4Address) java.net.Inet4Address.getByName(host);
        result = ip.getHostAddress();
      } catch (Exception e) {
        result = "Unknown";
      }

      out.buffer = buffer;
      out.start = 0;
      out.end = result.getBytes().length;
      buffer.setBytes(0, result.getBytes());
    }
  }

  /* This function gets the host name associated with a domain */
  @FunctionTemplate(name = "get_mx_record", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)

  public static class MXRecordFunction implements DrillSimpleFunc {

    @Param
    VarCharHolder ipaddress;

    @Output
    VarCharHolder out;

    @Inject
    DrillBuf buffer;

    @Override
    public void setup() {

    }

    @Override
    public void eval() {
      String ipString = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(ipaddress.start, ipaddress.end, ipaddress.buffer);
      String MXRecordName = "";
      int minPriority = 1000000000;
      try {
        org.xbill.DNS.Record[] records = new org.xbill.DNS.Lookup(ipString, org.xbill.DNS.Type.MX).run();
        for (int i = 0; i < records.length; i++) {
          org.xbill.DNS.MXRecord mx = (org.xbill.DNS.MXRecord) records[i];

          //Assign the return value to the MX record with the lowest priority (the primary record)
          if (mx.getPriority() < minPriority) {
            minPriority = mx.getPriority();
            MXRecordName = mx.getTarget().toString();
          }
          //System.out.println("Nameserver " + mx.getTarget() + " " + mx.getPriority());
        }
      } catch (Exception e) {
        MXRecordName = "MX Record not found";
      }

      out.buffer = buffer;
      out.start = 0;
      out.end = MXRecordName.getBytes().length;
      buffer.setBytes(0, MXRecordName.getBytes());
    }
  }

  /* This function gets the host name associated with an IP address */
  @FunctionTemplate(name = "get_mx_records",
      scope = FunctionTemplate.FunctionScope.SIMPLE)
  public static class MXRecordListFunction implements DrillSimpleFunc {
    @Param
    VarCharHolder ipaddress;

    @Output
    BaseWriter.ComplexWriter out;

    @Inject
    DrillBuf buffer;

    @Override
    public void setup() {

    }

    @Override
    public void eval() {
      String domainName = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(ipaddress.start, ipaddress.end, ipaddress.buffer);

      org.apache.drill.exec.vector.complex.writer.BaseWriter.ListWriter listWriter = out.rootAsList();
      try {
        org.xbill.DNS.Record[] records = new org.xbill.DNS.Lookup(domainName, org.xbill.DNS.Type.MX).run();
        String mxRecordName;

        for (int i = 0; i < records.length; i++) {
          org.xbill.DNS.MXRecord mx = (org.xbill.DNS.MXRecord) records[i];
          org.apache.drill.exec.expr.holders.VarCharHolder rowHolder = new org.apache.drill.exec.expr.holders.VarCharHolder();
          mxRecordName = mx.getTarget().toString();

          byte[] rowStringBytes = mxRecordName.getBytes();
          buffer.reallocIfNeeded(rowStringBytes.length);
          buffer.setBytes(0, rowStringBytes);

          rowHolder.start = 0;
          rowHolder.end = rowStringBytes.length;
          rowHolder.buffer = buffer;

          listWriter.varChar().write(rowHolder);
        }
      } catch (Exception e) {
        System.out.println("Can't find records for " + domainName + "\n" + e.getMessage());
      }
    }
  }

  /* This function performs a complete DNS lookup */
  @FunctionTemplate(names = {"dns_lookup", "dnsLookup", "dns"}, scope = FunctionTemplate.FunctionScope.SIMPLE)
  public static class DNSLookupFunctionWithNull implements DrillSimpleFunc {

    @Param
    NullableVarCharHolder rawDomainName;

    @Output
    BaseWriter.ComplexWriter out;

    @Inject
    DrillBuf buffer;

    @Override
    public void setup() {
      // no op
    }

    @Override
    public void eval() {
      if (rawDomainName.isSet == 0) {
        org.apache.drill.exec.vector.complex.writer.BaseWriter.ListWriter listWriter = out.rootAsList();
        org.apache.drill.exec.vector.complex.writer.BaseWriter.MapWriter rowMapWriter = listWriter.map();
        listWriter.startList();
        rowMapWriter.start();
        rowMapWriter.end();
        listWriter.endList();
        return;
      }

      try {
        String domainName = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(rawDomainName.start, rawDomainName.end, rawDomainName.buffer);
        org.apache.drill.exec.udfs.DNSUtils.getDNS(domainName, out, buffer);
      } catch (Exception e) {
        org.apache.drill.exec.vector.complex.writer.BaseWriter.ListWriter listWriter = out.rootAsList();
        org.apache.drill.exec.vector.complex.writer.BaseWriter.MapWriter rowMapWriter = listWriter.map();
        listWriter.startList();
        rowMapWriter.start();
        rowMapWriter.end();
        listWriter.endList();
      }
    }
  }

  @FunctionTemplate(names = {"dns_lookup", "dnsLookup", "dns"}, scope = FunctionTemplate.FunctionScope.SIMPLE)
  public static class DNSLookupFunctionWithResolver implements DrillSimpleFunc {

    @Param
    NullableVarCharHolder rawDomainName;

    @Param(constant = true)
    VarCharHolder resolverHolder;

    @Output
    BaseWriter.ComplexWriter out;

    @Workspace
    String resolver;

    @Inject
    DrillBuf buffer;

    @Override
    public void setup() {
      resolver = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(resolverHolder.start, resolverHolder.end, resolverHolder.buffer);
    }

    @Override
    public void eval() {
      if (rawDomainName.isSet == 0) {
        org.apache.drill.exec.vector.complex.writer.BaseWriter.ListWriter listWriter = out.rootAsList();
        org.apache.drill.exec.vector.complex.writer.BaseWriter.MapWriter rowMapWriter = listWriter.map();
        listWriter.startList();
        rowMapWriter.start();
        rowMapWriter.end();
        listWriter.endList();
        return;
      }

      try {
        String domainName = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(rawDomainName.start, rawDomainName.end, rawDomainName.buffer);
        org.apache.drill.exec.udfs.DNSUtils.getDNS(domainName, resolver, out, buffer);
      } catch (Exception e) {
        org.apache.drill.exec.vector.complex.writer.BaseWriter.ListWriter listWriter = out.rootAsList();
        org.apache.drill.exec.vector.complex.writer.BaseWriter.MapWriter rowMapWriter = listWriter.map();
        listWriter.startList();
        rowMapWriter.start();
        rowMapWriter.end();
        listWriter.endList();
      }
    }
  }

  @FunctionTemplate(names = {"whois"}, scope = FunctionTemplate.FunctionScope.SIMPLE)
  public static class WhoIsFunction implements DrillSimpleFunc {

    @Param
    NullableVarCharHolder rawDomainName;

    @Output
    BaseWriter.ComplexWriter out;

    @Inject
    DrillBuf buffer;
    @Override
    public void setup() {

    }

    @Override
    public void eval() {
      String domain = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(rawDomainName.start, rawDomainName.end, rawDomainName.buffer);
      org.apache.drill.exec.udfs.DNSUtils.whois(domain, out, buffer);
    }
  }
}
