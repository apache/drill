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
import org.apache.drill.exec.expr.holders.VarCharHolder;
import org.apache.drill.exec.expr.holders.IntHolder;
import org.apache.drill.exec.expr.holders.BitHolder;
import org.apache.drill.exec.expr.holders.Float8Holder;
import org.apache.drill.exec.expr.holders.VarBinaryHolder;


import javax.inject.Inject;

public class GeoIPFunctions {

  private GeoIPFunctions() {
  }

  @FunctionTemplate(name = "getCountryName", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)

  public static class getCountryNameFunction implements DrillSimpleFunc {

    @Param
    VarCharHolder inputTextA;

    @Output
    VarCharHolder out;

    @Inject
    DrillBuf buffer;

    @Workspace
    com.maxmind.geoip2.DatabaseReader reader;

    public void setup() {
      reader = org.apache.drill.exec.udfs.SecurityHelperFunctions.getCountryDatabaseReader();
    }

    public void eval() {
      String ip = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(inputTextA.start, inputTextA.end, inputTextA.buffer);
      String countryName = "Unknown";

      try {
        com.maxmind.geoip2.model.CountryResponse country = reader.country(java.net.InetAddress.getByName(ip));

        countryName = country.getCountry().getName();
        if (countryName == null) {
          countryName = "Unknown";
        }
      } catch (Exception e) {
        countryName = "Unknown";
      }

      out.buffer = buffer;
      out.start = 0;
      out.end = countryName.getBytes().length;
      buffer.setBytes(0, countryName.getBytes());
    }
  }


  @FunctionTemplate(name = "getCountryISOCode", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class getCountryISOFunction implements DrillSimpleFunc {

    @Param
    VarCharHolder inputTextA;

    @Output
    VarCharHolder out;

    @Inject
    DrillBuf buffer;

    @Workspace
    com.maxmind.geoip2.DatabaseReader reader;

    public void setup() {
      reader = org.apache.drill.exec.udfs.SecurityHelperFunctions.getCountryDatabaseReader();
    }

    public void eval() {
      String ip = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(inputTextA.start, inputTextA.end, inputTextA.buffer);
      String countryName = "UNK";

      try {
        com.maxmind.geoip2.model.CountryResponse country = reader.country(java.net.InetAddress.getByName(ip));
        countryName = country.getCountry().getIsoCode();
        if (countryName == null) {
          countryName = "UNK";
        }

      } catch (Exception e) {
        countryName = "UNK";
      }
      out.buffer = buffer;
      out.start = 0;
      out.end = countryName.getBytes().length;
      buffer.setBytes(0, countryName.getBytes());
    }
  }


  @FunctionTemplate(name = "getCountryConfidence", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class getCountryConfidenceFunction implements DrillSimpleFunc {

    @Param
    VarCharHolder inputTextA;

    @Output
    IntHolder out;

    @Inject
    DrillBuf buffer;

    @Workspace
    com.maxmind.geoip2.DatabaseReader reader;

    public void setup() {
      reader = org.apache.drill.exec.udfs.SecurityHelperFunctions.getCountryDatabaseReader();
    }

    public void eval() {
      String ip = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(inputTextA.start, inputTextA.end, inputTextA.buffer);
      int confidence = 0;

      try {
        com.maxmind.geoip2.model.CountryResponse country = reader.country(java.net.InetAddress.getByName(ip));
        confidence = country.getCountry().getConfidence();

      } catch (Exception e) {
        confidence = 0;
      }
      out.value = confidence;
    }
  }


  @FunctionTemplate(name = "getCityName", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)

  public static class getCityNameFunction implements DrillSimpleFunc {

    @Param
    VarCharHolder inputTextA;

    @Output
    VarCharHolder out;

    @Inject
    DrillBuf buffer;

    @Workspace
    com.maxmind.geoip2.DatabaseReader reader;

    public void setup() {
      reader = org.apache.drill.exec.udfs.SecurityHelperFunctions.getCityDatabaseReader();
    }

    public void eval() {
      String ip = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(inputTextA.start, inputTextA.end, inputTextA.buffer);
      String cityName = "";

      try {
        com.maxmind.geoip2.model.CityResponse city = reader.city(java.net.InetAddress.getByName(ip));
        cityName = city.getCity().getName();
        if (cityName == null) {
          cityName = "Unknown";
        }
      } catch (Exception e) {
        cityName = "Unknown";
      }
      out.buffer = buffer;
      out.start = 0;
      out.end = cityName.getBytes().length;
      buffer.setBytes(0, cityName.getBytes());
    }
  }

  @FunctionTemplate(name = "getCityConfidence", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)

  public static class getCityConfidenceFunction implements DrillSimpleFunc {

    @Param
    VarCharHolder inputTextA;

    @Output
    IntHolder out;

    @Inject
    DrillBuf buffer;

    @Workspace
    com.maxmind.geoip2.DatabaseReader reader;

    public void setup() {
      reader = org.apache.drill.exec.udfs.SecurityHelperFunctions.getCityDatabaseReader();
    }

    public void eval() {
      String ip = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(inputTextA.start, inputTextA.end, inputTextA.buffer);
      int cityConfidence = 0;

      try {
        com.maxmind.geoip2.model.CityResponse city = reader.city(java.net.InetAddress.getByName(ip));
        cityConfidence = city.getCity().getConfidence();
      } catch (Exception e) {
        cityConfidence = 0;
      }
      out.value = cityConfidence;
    }
  }

  @FunctionTemplate(name = "getLatitude", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class getLatitudeFunction implements DrillSimpleFunc {

    @Param
    VarCharHolder inputTextA;

    @Output
    Float8Holder out;

    @Inject
    DrillBuf buffer;

    @Workspace
    com.maxmind.geoip2.DatabaseReader reader;

    public void setup() {
      reader = org.apache.drill.exec.udfs.SecurityHelperFunctions.getCityDatabaseReader();
    }


    public void eval() {
      String ip = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(inputTextA.start, inputTextA.end, inputTextA.buffer);
      double latitude = 0.0;

      try {
        com.maxmind.geoip2.model.CityResponse city = reader.city(java.net.InetAddress.getByName(ip));
        com.maxmind.geoip2.record.Location location = city.getLocation();
        latitude = location.getLatitude();

      } catch (Exception e) {
        latitude = 0.0;
      }

      out.value = latitude;

    }
  }

  @FunctionTemplate(name = "getLongitude", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class getLongitudeFunction implements DrillSimpleFunc {

    @Param
    VarCharHolder inputTextA;

    @Output
    Float8Holder out;

    @Workspace
    com.maxmind.geoip2.DatabaseReader reader;

    @Workspace
    java.io.File database;

    public void setup() {
      reader = org.apache.drill.exec.udfs.SecurityHelperFunctions.getCityDatabaseReader();
    }


    public void eval() {
      String ip = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(inputTextA.start, inputTextA.end, inputTextA.buffer);
      double longitude = 0.0;

      try {
        com.maxmind.geoip2.model.CityResponse city = reader.city(java.net.InetAddress.getByName(ip));
        com.maxmind.geoip2.record.Location location = city.getLocation();
        longitude = location.getLongitude();

      } catch (Exception e) {
        longitude = 0.0;
      }

      out.value = longitude;

    }
  }

  @FunctionTemplate(name = "getTimezone", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class getTimezoneFunction implements DrillSimpleFunc {

    @Param
    VarCharHolder inputTextA;

    @Output
    VarCharHolder out;

    @Inject
    DrillBuf buffer;

    @Workspace
    com.maxmind.geoip2.DatabaseReader reader;

    @Workspace
    java.io.File database;

    public void setup() {
      reader = org.apache.drill.exec.udfs.SecurityHelperFunctions.getCityDatabaseReader();
    }

    public void eval() {
      String ip = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(inputTextA.start, inputTextA.end, inputTextA.buffer);
      String timezone = "Unknown";

      try {
        com.maxmind.geoip2.model.CityResponse city = reader.city(java.net.InetAddress.getByName(ip));
        com.maxmind.geoip2.record.Location location = city.getLocation();
        timezone = location.getTimeZone();

      } catch (Exception e) {
        timezone = "Unknown";
      }

      out.buffer = buffer;
      out.start = 0;
      out.end = timezone.getBytes().length;
      buffer.setBytes(0, timezone.getBytes());

    }
  }

  @FunctionTemplate(name = "getAccuracyRadius", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class getAccuracyRadiusFunction implements DrillSimpleFunc {

    @Param
    VarCharHolder inputTextA;

    @Output
    IntHolder out;

    @Inject
    DrillBuf buffer;

    @Workspace
    com.maxmind.geoip2.DatabaseReader reader;

    @Workspace
    java.io.File database;

    public void setup() {
      reader = org.apache.drill.exec.udfs.SecurityHelperFunctions.getCityDatabaseReader();
    }

    public void eval() {
      String ip = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(inputTextA.start, inputTextA.end, inputTextA.buffer);
      int accuracyRadius = 0;

      try {
        com.maxmind.geoip2.model.CityResponse city = reader.city(java.net.InetAddress.getByName(ip));
        com.maxmind.geoip2.record.Location location = city.getLocation();
        accuracyRadius = location.getAccuracyRadius();

      } catch (Exception e) {
        accuracyRadius = 0;
      }
      out.value = accuracyRadius;
    }
  }

  @FunctionTemplate(name = "getAverageIncome", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class getAverageIncomeFunction implements DrillSimpleFunc {

    @Param
    VarCharHolder inputTextA;

    @Output
    IntHolder out;

    @Workspace
    com.maxmind.geoip2.DatabaseReader reader;

    @Workspace
    java.io.File database;

    public void setup() {
      reader = org.apache.drill.exec.udfs.SecurityHelperFunctions.getCityDatabaseReader();
    }

    public void eval() {
      String ip = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(inputTextA.start, inputTextA.end, inputTextA.buffer);
      int averageIncome = 0;

      try {
        com.maxmind.geoip2.model.CityResponse city = reader.city(java.net.InetAddress.getByName(ip));
        com.maxmind.geoip2.record.Location location = city.getLocation();
        averageIncome = location.getAverageIncome();

      } catch (Exception e) {
        averageIncome = 0;
      }
      out.value = averageIncome;
    }
  }

  @FunctionTemplate(name = "getMetroCode", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class getMetroCodeFunction implements DrillSimpleFunc {

    @Param
    VarCharHolder inputTextA;

    @Output
    IntHolder out;

    @Workspace
    com.maxmind.geoip2.DatabaseReader reader;

    @Workspace
    java.io.File database;

    public void setup() {
      reader = org.apache.drill.exec.udfs.SecurityHelperFunctions.getCityDatabaseReader();
    }

    public void eval() {
      String ip = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(inputTextA.start, inputTextA.end, inputTextA.buffer);
      int metroCode = 0;

      try {
        com.maxmind.geoip2.model.CityResponse city = reader.city(java.net.InetAddress.getByName(ip));
        com.maxmind.geoip2.record.Location location = city.getLocation();
        metroCode = location.getMetroCode();

      } catch (Exception e) {
        metroCode = 0;
      }
      out.value = metroCode;
    }
  }

  @FunctionTemplate(name = "getPopulationDensity", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class getPopulationDensityFunction implements DrillSimpleFunc {

    @Param
    VarCharHolder inputTextA;

    @Output
    IntHolder out;

    @Workspace
    com.maxmind.geoip2.DatabaseReader reader;

    public void setup() {
      reader = org.apache.drill.exec.udfs.SecurityHelperFunctions.getCityDatabaseReader();
    }

    public void eval() {
      String ip = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(inputTextA.start, inputTextA.end, inputTextA.buffer);
      int populationDensity = 0;

      try {
        com.maxmind.geoip2.model.CityResponse city = reader.city(java.net.InetAddress.getByName(ip));
        com.maxmind.geoip2.record.Location location = city.getLocation();
        populationDensity = location.getPopulationDensity();

      } catch (Exception e) {
        populationDensity = 0;
      }
      out.value = populationDensity;
    }
  }

  @FunctionTemplate(names = {"isEU", "isEuropeanUnion"}, scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class isEUFunction implements DrillSimpleFunc {

    @Param
    VarCharHolder inputTextA;

    @Output
    BitHolder out;

    @Workspace
    com.maxmind.geoip2.DatabaseReader reader;

    public void setup() {
      reader = org.apache.drill.exec.udfs.SecurityHelperFunctions.getCountryDatabaseReader();
    }

    public void eval() {
      String ip = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(inputTextA.start, inputTextA.end, inputTextA.buffer);
      boolean isEU = false;

      try {
        com.maxmind.geoip2.model.CountryResponse country = reader.country(java.net.InetAddress.getByName(ip));
        isEU = country.getCountry().isInEuropeanUnion();
      } catch (Exception e) {
        isEU = false;
      }
      if (isEU) {
        out.value = 1;
      } else {
        out.value = 0;
      }
    }
  }

  @FunctionTemplate(name = "getPostalCode", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class getPostalCodeFunction implements DrillSimpleFunc {

    @Param
    VarCharHolder inputTextA;

    @Output
    VarCharHolder out;

    @Inject
    DrillBuf buffer;

    @Workspace
    com.maxmind.geoip2.DatabaseReader reader;


    public void setup() {
      reader = org.apache.drill.exec.udfs.SecurityHelperFunctions.getCityDatabaseReader();
    }


    public void eval() {
      String ip = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(inputTextA.start, inputTextA.end, inputTextA.buffer);
      String postalCode = "";

      try {
        com.maxmind.geoip2.model.CityResponse city = reader.city(java.net.InetAddress.getByName(ip));
        com.maxmind.geoip2.record.Postal postal = city.getPostal();
        postalCode = postal.getCode();
        if (postalCode == null) {
          postalCode = "Unknown";
        }
      } catch (Exception e) {
        postalCode = "Unknown";
      }

      out.buffer = buffer;
      out.start = 0;
      out.end = postalCode.getBytes().length;
      buffer.setBytes(0, postalCode.getBytes());

    }
  }

  @FunctionTemplate(name = "getCoordPoint", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class getCoordPointFunction implements DrillSimpleFunc {

    @Param
    VarCharHolder inputTextA;

    @Output
    VarBinaryHolder out;

    @Inject
    DrillBuf buffer;

    @Workspace
    com.maxmind.geoip2.DatabaseReader reader;


    public void setup() {
      reader = org.apache.drill.exec.udfs.SecurityHelperFunctions.getCityDatabaseReader();
    }


    public void eval() {
      String ip = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(inputTextA.start, inputTextA.end, inputTextA.buffer);
      double longitude = 0.0;
      double latitude = 0.0;

      try {
        com.maxmind.geoip2.model.CityResponse city = reader.city(java.net.InetAddress.getByName(ip));
        com.maxmind.geoip2.record.Location location = city.getLocation();
        longitude = location.getLongitude();
        latitude = location.getLatitude();

      } catch (Exception e) {
        latitude = 0.0;
        longitude = 0.0;
      }
      com.esri.core.geometry.ogc.OGCPoint point = new com.esri.core.geometry.ogc.OGCPoint(new com.esri.core.geometry.Point(longitude, latitude), com.esri.core.geometry.SpatialReference.create(4326));

      java.nio.ByteBuffer pointBytes = point.asBinary();
      out.buffer = buffer;
      out.start = 0;
      out.end = pointBytes.remaining();
      buffer.setBytes(0, pointBytes);

    }
  }

  @FunctionTemplate(name = "getASN", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class getASNFunction implements DrillSimpleFunc {

    @Param
    VarCharHolder inputTextA;

    @Output
    IntHolder out;

    @Workspace
    com.maxmind.geoip2.DatabaseReader reader;

    public void setup() {
      reader = org.apache.drill.exec.udfs.SecurityHelperFunctions.getASNDatabaseReader();
    }

    public void eval() {
      String ip = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(inputTextA.start, inputTextA.end, inputTextA.buffer);
      int ASN = 0;

      try {
        ASN = reader.asn(java.net.InetAddress.getByName(ip)).getAutonomousSystemNumber();
      } catch (Exception e) {
        ASN = 0;
      }
      out.value = ASN;
    }
  }

  @FunctionTemplate(name = "getASNOrganization", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class getASNOrgFunction implements DrillSimpleFunc {

    @Param
    VarCharHolder inputTextA;

    @Output
    VarCharHolder out;

    @Inject
    DrillBuf buffer;

    @Workspace
    com.maxmind.geoip2.DatabaseReader reader;

    public void setup() {
      reader = org.apache.drill.exec.udfs.SecurityHelperFunctions.getASNDatabaseReader();
    }

    public void eval() {
      String ip = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(inputTextA.start, inputTextA.end, inputTextA.buffer);
      String ASNorg = "Unknown";

      try {
        ASNorg = reader.asn(java.net.InetAddress.getByName(ip)).getAutonomousSystemOrganization();
      } catch (Exception e) {
        ASNorg = "Unknown";
      }
      out.buffer = buffer;
      out.start = 0;
      out.end = ASNorg.getBytes().length;
      buffer.setBytes(0, ASNorg.getBytes());
    }
  }

  @FunctionTemplate(name = "isAnonymous", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class isAnonymousFunction implements DrillSimpleFunc {

    @Param
    VarCharHolder inputTextA;

    @Output
    BitHolder out;

    @Workspace
    com.maxmind.geoip2.DatabaseReader reader;

    public void setup() {
      reader = org.apache.drill.exec.udfs.SecurityHelperFunctions.getCityDatabaseReader();
    }

    public void eval() {
      String ip = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(inputTextA.start, inputTextA.end, inputTextA.buffer);
      boolean isAnonymous = false;

      try {
        com.maxmind.geoip2.model.AnonymousIpResponse response = reader.anonymousIp(java.net.InetAddress.getByName(ip));
        isAnonymous = response.isAnonymous();
      } catch (Exception e) {
        isAnonymous = false;
      }
      if (isAnonymous) {
        out.value = 1;
      } else {
        out.value = 0;
      }
    }
  }

  @FunctionTemplate(name = "isAnonymousVPN", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class isAnonymousVPNFunction implements DrillSimpleFunc {

    @Param
    VarCharHolder inputTextA;

    @Output
    BitHolder out;

    @Workspace
    com.maxmind.geoip2.DatabaseReader reader;

    public void setup() {
      reader = org.apache.drill.exec.udfs.SecurityHelperFunctions.getCityDatabaseReader();
    }

    public void eval() {
      String ip = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(inputTextA.start, inputTextA.end, inputTextA.buffer);
      boolean isAnonymousVPN = false;

      try {
        com.maxmind.geoip2.model.AnonymousIpResponse response = reader.anonymousIp(java.net.InetAddress.getByName(ip));
        isAnonymousVPN = response.isAnonymousVpn();
      } catch (Exception e) {
        isAnonymousVPN = false;
      }
      if (isAnonymousVPN) {
        out.value = 1;
      } else {
        out.value = 0;
      }
    }
  }

  @FunctionTemplate(name = "isHostingProvider", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class isHostingProviderFunction implements DrillSimpleFunc {

    @Param
    VarCharHolder inputTextA;

    @Output
    BitHolder out;

    @Workspace
    com.maxmind.geoip2.DatabaseReader reader;

    public void setup() {
      reader = org.apache.drill.exec.udfs.SecurityHelperFunctions.getCityDatabaseReader();
    }

    public void eval() {
      String ip = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(inputTextA.start, inputTextA.end, inputTextA.buffer);
      boolean isHostingProvider = false;

      try {
        com.maxmind.geoip2.model.AnonymousIpResponse response = reader.anonymousIp(java.net.InetAddress.getByName(ip));
        isHostingProvider = response.isHostingProvider();
      } catch (Exception e) {
        isHostingProvider = false;
      }
      if (isHostingProvider) {
        out.value = 1;
      } else {
        out.value = 0;
      }
    }
  }

  @FunctionTemplate(name = "isPublicProxy", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class isPublicProxyFunction implements DrillSimpleFunc {

    @Param
    VarCharHolder inputTextA;

    @Output
    BitHolder out;

    @Workspace
    com.maxmind.geoip2.DatabaseReader reader;

    public void setup() {
      reader = org.apache.drill.exec.udfs.SecurityHelperFunctions.getCityDatabaseReader();
    }

    public void eval() {
      String ip = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(inputTextA.start, inputTextA.end, inputTextA.buffer);
      boolean isPublicProxy = false;

      try {
        com.maxmind.geoip2.model.AnonymousIpResponse response = reader.anonymousIp(java.net.InetAddress.getByName(ip));
        isPublicProxy = response.isPublicProxy();
      } catch (Exception e) {
        isPublicProxy = false;
      }
      if (isPublicProxy) {
        out.value = 1;
      } else {
        out.value = 0;
      }
    }
  }

  @FunctionTemplate(name = "isTORExitNode", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class isTORFunction implements DrillSimpleFunc {

    @Param
    VarCharHolder inputTextA;

    @Output
    BitHolder out;

    @Workspace
    com.maxmind.geoip2.DatabaseReader reader;

    public void setup() {
      reader = org.apache.drill.exec.udfs.SecurityHelperFunctions.getCityDatabaseReader();
    }

    public void eval() {
      String ip = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(inputTextA.start, inputTextA.end, inputTextA.buffer);
      boolean isTOR = false;

      try {
        com.maxmind.geoip2.model.AnonymousIpResponse response = reader.anonymousIp(java.net.InetAddress.getByName(ip));
        isTOR = response.isTorExitNode();
      } catch (Exception e) {
        isTOR = false;
      }
      if (isTOR) {
        out.value = 1;
      } else {
        out.value = 0;
      }
    }
  }
}
