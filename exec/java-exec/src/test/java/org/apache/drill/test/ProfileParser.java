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
 ******************************************************************************/
package org.apache.drill.test;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonNumber;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonValue;

/**
 * Parses a query profile and provides access to various bits of the profile
 * for diagnostic purposes during tests.
 */

public class ProfileParser {

  JsonObject profile;
  List<String> plans;

  public ProfileParser( File file ) throws IOException {
    try (FileReader fileReader = new FileReader(file);
         JsonReader reader = Json.createReader(fileReader)) {
      profile = (JsonObject) reader.read();
    }
  }

  public String getQuery( ) {
    return profile.get("query").toString();
  }

  public String getPlan() {
    return profile.get("plan").toString();
  }

  public List<String> getPlans() {
    if ( plans != null ) {
      return plans; }
    String plan = getPlan( );
    Pattern p = Pattern.compile( "(\\d\\d-\\d+[^\\\\]*)\\\\n", Pattern.MULTILINE );
    Matcher m = p.matcher(plan);
    plans = new ArrayList<>( );
    while ( m.find() ) {
      plans.add(m.group(1));
    }
    return plans;
  }

  public List<String> getScans( ) {
    List<String> scans = new ArrayList<>();
    int n = getPlans( ).size();
//    Pattern p = Pattern.compile( "\\d+-\\d+\\s+(\\w+)\\(" );
    for ( int i = n-1; i >= 0;  i-- ) {
      String plan = plans.get( i );
//      Matcher m = p.matcher( plan );
//      if ( ! m.find() ) { continue; }
      if ( plan.contains( " Scan(" ) ) {
        scans.add( plan );
      }
    }
    return scans;
  }

  public List<FieldDef> getColumns( String plan ) {
    Pattern p = Pattern.compile( "RecordType\\((.*)\\):" );
    Matcher m = p.matcher(plan);
    if ( ! m.find() ) { return null; }
    String frag = m.group(1);
    String parts[] = frag.split( ", " );
    List<FieldDef> fields = new ArrayList<>( );
    for ( String part : parts ) {
      String halves[] = part.split( " " );
      fields.add( new FieldDef( halves[1], halves[0] ) );
    }
    return fields;
  }

  public Map<Integer,String> getOperators( ) {
    Map<Integer,String> ops = new HashMap<>();
    int n = getPlans( ).size();
    Pattern p = Pattern.compile( "\\d+-(\\d+)\\s+(\\w+)" );
    for ( int i = n-1; i >= 0;  i-- ) {
      String plan = plans.get( i );
      Matcher m = p.matcher( plan );
      if ( ! m.find() ) { continue; }
      int index = Integer.parseInt(m.group(1));
      String op = m.group(2);
      ops.put(index,op);
    }
    return ops;
  }

  public JsonArray getFragmentProfile( ) {
    return profile.getJsonArray("fragmentProfile");
  }

  public static class OpInfo {
    int opId;
    int type;
    String name;
    long processMs;
    long waitMs;
    long setupMs;
    long peakMem;
    Map<Integer,JsonValue> metrics = new HashMap<>();

    public long getMetric(int id) {
      JsonValue value = metrics.get(id);
      if (value == null) {
        return 0; }
      return ((JsonNumber) value).longValue();
    }
  }

  public Map<Integer,OpInfo> getOpInfo( ) {
    Map<Integer,String> ops = getOperators( );
    Map<Integer,OpInfo> info = new HashMap<>( );
    JsonArray frags = getFragmentProfile( );
    JsonObject fragProfile = frags.getJsonObject(0).getJsonArray("minorFragmentProfile").getJsonObject(0);
    JsonArray opList = fragProfile.getJsonArray("operatorProfile");
    for ( JsonObject opProfile : opList.getValuesAs(JsonObject.class) ) {
      parseOpProfile( ops, info, opProfile );
    }
    return info;
  }

  private void parseOpProfile(Map<Integer, String> ops,
      Map<Integer, OpInfo> info, JsonObject opProfile) {
    OpInfo opInfo = new OpInfo( );
    opInfo.opId = opProfile.getInt("operatorId");
    opInfo.type = opProfile.getInt("operatorType");
    opInfo.name = ops.get(opInfo.opId);
    opInfo.processMs = opProfile.getJsonNumber("processNanos").longValue() / 1_000_000;
    opInfo.waitMs = opProfile.getJsonNumber("waitNanos").longValue() / 1_000_000;
    opInfo.setupMs = opProfile.getJsonNumber("setupNanos").longValue() / 1_000_000;
    opInfo.peakMem = opProfile.getJsonNumber("peakLocalMemoryAllocated").longValue() / (1024 * 1024);
    JsonArray array = opProfile.getJsonArray("metric");
    if (array != null) {
      for (int i = 0; i < array.size(); i++) {
        JsonObject metric = array.getJsonObject(i);
        opInfo.metrics.put(metric.getJsonNumber("metricId").intValue(), metric.get("longValue"));
      }
    }
    info.put(opInfo.opId, opInfo);
  }

  public void print() {
    Map<Integer, OpInfo> opInfo = getOpInfo();
    int n = opInfo.size();
    long totalSetup = 0;
    long totalProcess = 0;
    for ( int i = 0;  i <= n;  i++ ) {
      OpInfo op = opInfo.get(i);
      if ( op == null ) { continue; }
      totalSetup += op.setupMs;
      totalProcess += op.processMs;
    }
    long total = totalSetup + totalProcess;
    for ( int i = 0;  i <= n;  i++ ) {
      OpInfo op = opInfo.get(i);
      if ( op == null ) { continue; }
      System.out.print( "Op: " );
      System.out.print( op.opId );
      System.out.println( " " + op.name );
      System.out.print( "  Setup:   " + op.setupMs );
      System.out.print( " - " + percent(op.setupMs, totalSetup ) + "%" );
      System.out.println( ", " + percent(op.setupMs, total ) + "%" );
      System.out.print( "  Process: " + op.processMs );
      System.out.print( " - " + percent(op.processMs, totalProcess ) + "%" );
      System.out.println( ", " + percent(op.processMs, total ) + "%" );
      if (op.type == 17) {
        long value = op.getMetric(0);
        System.out.println( "  Spills: " + value );
      }
      if (op.waitMs > 0) {
        System.out.println( "  Wait:    " + op.waitMs );
      }
      if ( op.peakMem > 0) {
        System.out.println( "  Memory: " + op.peakMem );
      }
    }
    System.out.println( "Total:" );
    System.out.println( "  Setup:   " + totalSetup );
    System.out.println( "  Process: " + totalProcess );
  }

  public static long percent( long value, long total ) {
    if ( total == 0 ) {
      return 0; }
    return Math.round(value * 100 / total );
  }

}
