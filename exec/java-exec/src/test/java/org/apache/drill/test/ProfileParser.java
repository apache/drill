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
import java.util.Collections;
import java.util.Comparator;
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

import org.apache.drill.exec.proto.UserBitShared.CoreOperatorType;

import com.google.common.base.Preconditions;

/**
 * Parses a query profile and provides access to various bits of the profile
 * for diagnostic purposes during tests.
 */

public class ProfileParser {

  JsonObject profile;
  String query;
  List<String> plans;
  List<OpDefInfo> operations;
  Map<Integer,FragInfo> fragments = new HashMap<>();
  private List<OpDefInfo> topoOrder;

  public ProfileParser( File file ) throws IOException {
    try (FileReader fileReader = new FileReader(file);
         JsonReader reader = Json.createReader(fileReader)) {
      profile = (JsonObject) reader.read();
    }

    parse();
  }

  private void parse() {
    parseQuery();
    parsePlans();
    buildFrags();
    parseFragProfiles();
    mapOpProfiles();
    aggregateOpers();
    buildTree();
  }

  private void parseQuery() {
    query = profile.getString("query");
    query = query.replace("//n", "\n");
  }

  /**
   * Parse a text version of the plan as it appears in the JSON
   * query profile.
   */

  private static class PlanParser {

    List<String> plans = new ArrayList<>();
    List<OpDefInfo> operations = new ArrayList<>();
    List<OpDefInfo> sorted = new ArrayList<>();

    public void parsePlans(String plan) {
      plans = new ArrayList<>( );
      String parts[] = plan.split("\n");
      for (String part : parts) {
        plans.add(part);
        OpDefInfo opDef = new OpDefInfo( part );
        operations.add(opDef);
      }
      sortList();
    }

    public void sortList() {
      List<OpDefInfo> raw = new ArrayList<>( );
      raw.addAll( operations );
      Collections.sort( raw, new Comparator<OpDefInfo>() {
        @Override
        public int compare(OpDefInfo o1, OpDefInfo o2) {
          int result = Integer.compare(o1.majorId, o2.majorId);
          if ( result == 0 ) {
            result = Integer.compare(o1.stepId, o2.stepId);
          }
          return result;
        }
      });
      int currentFrag = 0;
      int currentStep = 0;
      for ( OpDefInfo opDef : raw ) {
        if ( currentFrag < opDef.majorId ) {
          currentFrag++;
          OpDefInfo sender = new OpDefInfo( currentFrag, 0 );
          sender.isInferred = true;
          sender.name = "Sender";
          sorted.add(sender);
          currentStep = 1;
          opDef.inferredParent = sender;
          sender.children.add( opDef );
        }
        if ( opDef.stepId > currentStep ) {
          OpDefInfo unknown = new OpDefInfo( currentFrag, currentStep );
          unknown.isInferred = true;
          unknown.name = "Unknown";
          sorted.add(unknown);
          opDef.inferredParent = unknown;
          unknown.children.add( opDef );
        }
        sorted.add( opDef );
        currentStep = opDef.stepId + 1;
      }
    }
  }

  /**
   * Parse the plan portion of the query profile.
   */

  private void parsePlans() {
    PlanParser parser = new PlanParser();
    String plan = getPlan( );
    parser.parsePlans(plan);
    plans = parser.plans;
    topoOrder = parser.operations;
    operations = parser.sorted;
  }

  private void buildFrags() {
    for (OpDefInfo opDef : operations) {
      FragInfo major = fragments.get(opDef.majorId);
      if (major == null) {
        major = new FragInfo(opDef.majorId);
        fragments.put(opDef.majorId, major);
      }
      major.ops.add(opDef);
    }
  }

  private static List<FieldDef> parseCols(String cols) {
    String parts[] = cols.split( ", " );
    List<FieldDef> fields = new ArrayList<>( );
    for ( String part : parts ) {
      String halves[] = part.split( " " );
      fields.add( new FieldDef( halves[1], halves[0] ) );
    }
    return fields;
  }

  private void parseFragProfiles() {
    JsonArray frags = getFragmentProfile( );
    for (JsonObject fragProfile : frags.getValuesAs(JsonObject.class)) {
      int mId = fragProfile.getInt("majorFragmentId");
      FragInfo major = fragments.get(mId);
      major.parse(fragProfile);
    }
  }

  private void mapOpProfiles() {
    for (FragInfo major : fragments.values()) {
      for (MinorFragInfo minor : major.minors) {
        minor.mapOpProfiles(major);
      }
    }
  }

  /**
   * A typical plan has many operator details across multiple
   * minor fragments. Aggregate these totals to the "master"
   * definition of each operator.
   */

  private void aggregateOpers() {
    for (FragInfo major : fragments.values()) {
      for (OpDefInfo opDef : major.ops) {
        for ( OperatorProfile op : opDef.opExecs) {
          Preconditions.checkState( major.id == op.majorFragId );
          Preconditions.checkState( opDef.stepId == op.opId );
          opDef.actualRows += op.records;
          opDef.actualBatches += op.batches;
          opDef.actualMemory += op.peakMem * 1024 * 1024;
        }
      }
    }
  }

  /**
   * Reconstruct the operator tree from parsed information.
   */

  public void buildTree() {
    int currentLevel = 0;
    OpDefInfo opStack[] = new OpDefInfo[topoOrder.size()];
    for (OpDefInfo opDef : topoOrder) {
      currentLevel = opDef.globalLevel;
      opStack[currentLevel] = opDef;
      if ( opDef.inferredParent == null ) {
        if (currentLevel > 0) {
          opStack[currentLevel-1].children.add(opDef);
        }
      } else {
        opStack[currentLevel-1].children.add(opDef.inferredParent);
      }
    }
  }


  public String getQuery( ) {
    return profile.getString("query");
  }

  public String getPlan() {
    return profile.getString("plan");
  }

  public List<String> getPlans() {
    return plans;
  }

  public List<String> getScans( ) {
    List<String> scans = new ArrayList<>();
    int n = getPlans( ).size();
    for ( int i = n-1; i >= 0;  i-- ) {
      String plan = plans.get( i );
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

  /**
   * Information for a fragment, including the operators
   * in that fragment and the set of minor fragments.
   */

  public static class FragInfo {
    public int baseLevel;
    public int id;
    public List<OpDefInfo> ops = new ArrayList<>( );
    public List<MinorFragInfo> minors = new ArrayList<>( );

    public FragInfo(int majorId) {
      this.id = majorId;
    }

    public OpDefInfo getRootOperator() {
      return ops.get(0);
    }

    public void parse(JsonObject fragProfile) {
      JsonArray minorList = fragProfile.getJsonArray("minorFragmentProfile");
      for ( JsonObject minorProfile : minorList.getValuesAs(JsonObject.class) ) {
        minors.add( new MinorFragInfo(id, minorProfile) );
      }
    }
  }

  /**
   * Information about a minor fragment as parsed from the profile.
   */

  public static class MinorFragInfo {
    public final int majorId;
    public final int id;
    public final List<OperatorProfile> ops = new ArrayList<>( );

    public MinorFragInfo(int majorId, JsonObject minorProfile) {
      this.majorId = majorId;
      id = minorProfile.getInt("minorFragmentId");
      JsonArray opList = minorProfile.getJsonArray("operatorProfile");
      for ( JsonObject opProfile : opList.getValuesAs(JsonObject.class)) {
        ops.add( new OperatorProfile( majorId, id, opProfile) );
      }
    }

    public void mapOpProfiles(FragInfo major) {
      for (OperatorProfile op : ops) {
        OpDefInfo opDef = major.ops.get(op.opId);
        if ( opDef == null ) {
          System.out.println( "Can't find operator def: " + major.id + "-" + op.opId);
          continue;
        }
        op.opName = CoreOperatorType.valueOf(op.type).name();
//        System.out.println( major.id + "-" + id + "-" + opDef.stepId + " - Def: " + opDef.name + " / Prof: " + op.opName );
        op.opName = op.opName.replace("_", " ");
        op.name = opDef.name;
        if (op.name.equalsIgnoreCase(op.opName)) {
          op.opName = null;
        }
        op.defn = opDef;
        opDef.opName = op.opName;
        opDef.opExecs.add(op);
      }
    }

  }

  /**
   * Detailed information about each operator within a minor fragment
   * for a major fragment. Gathers the detailed information from
   * the profile.
   */

  public static class OperatorProfile {
    public OpDefInfo defn;
    public String opName;
    public int majorFragId;
    public int minorFragId;
    public int opId;
    public int type;
    public String name;
    public long processMs;
    public long waitMs;
    public long setupMs;
    public long peakMem;
    public Map<Integer,JsonNumber> metrics = new HashMap<>();
    public long records;
    public int batches;
    public int schemas;

    public OperatorProfile(int majorId, int minorId, JsonObject opProfile) {
      majorFragId = majorId;
      minorFragId = minorId;
      opId = opProfile.getInt("operatorId");
      type = opProfile.getInt("operatorType");
      processMs = opProfile.getJsonNumber("processNanos").longValue() / 1_000_000;
      waitMs = opProfile.getJsonNumber("waitNanos").longValue() / 1_000_000;
      setupMs = opProfile.getJsonNumber("setupNanos").longValue() / 1_000_000;
      peakMem = opProfile.getJsonNumber("peakLocalMemoryAllocated").longValue() / (1024 * 1024);
      JsonArray array = opProfile.getJsonArray("inputProfile");
      if (array != null) {
        for (int i = 0; i < array.size(); i++) {
          JsonObject obj = array.getJsonObject(i);
          records += obj.getJsonNumber("records").longValue();
          batches += obj.getInt("batches");
          schemas += obj.getInt("schemas");
        }
      }
      array = opProfile.getJsonArray("metric");
      if (array != null) {
        for (int i = 0; i < array.size(); i++) {
          JsonObject metric = array.getJsonObject(i);
          metrics.put(metric.getJsonNumber("metricId").intValue(), metric.getJsonNumber("longValue"));
        }
      }
    }

    public long getMetric(int id) {
      JsonValue value = metrics.get(id);
      if (value == null) {
        return 0; }
      return ((JsonNumber) value).longValue();
    }
  }

  /**
   * Information about an operator definition: the plan-time information
   * that appears in the plan portion of the profile. Also holds the
   * "actuals" from the minor fragment portion of the profile.
   * Allows integrating the "planned" vs. "actual" performance of the
   * query.
   */

  public static class OpDefInfo {
    public String opName;
    public boolean isInferred;
    public int majorId;
    public int stepId;
    public String args;
    public List<FieldDef> columns;
    public int globalLevel;
    public int localLevel;
    public int id;
    public int branchId;
    public boolean isBranchRoot;
    public double estMemoryCost;
    public double estNetCost;
    public double estIOCost;
    public double estCpuCost;
    public double estRowCost;
    public double estRows;
    public String name;
    public long actualMemory;
    public int actualBatches;
    public long actualRows;
    public OpDefInfo inferredParent;
    public List<OperatorProfile> opExecs = new ArrayList<>( );
    public List<OpDefInfo> children = new ArrayList<>( );

    // 00-00    Screen : rowType = RecordType(VARCHAR(10) Year, VARCHAR(65536) Month, VARCHAR(100) Devices, VARCHAR(100) Tier, VARCHAR(100) LOB, CHAR(10) Gateway, BIGINT Day, BIGINT Hour, INTEGER Week, VARCHAR(100) Week_end_date, BIGINT Usage_Cnt): \
    // rowcount = 100.0, cumulative cost = {7.42124276972414E9 rows, 7.663067406383167E10 cpu, 0.0 io, 2.24645048816E10 network, 2.692766612982188E8 memory}, id = 129302
    //
    // 00-01      Project(Year=[$0], Month=[$1], Devices=[$2], Tier=[$3], LOB=[$4], Gateway=[$5], Day=[$6], Hour=[$7], Week=[$8], Week_end_date=[$9], Usage_Cnt=[$10]) :
    // rowType = RecordType(VARCHAR(10) Year, VARCHAR(65536) Month, VARCHAR(100) Devices, VARCHAR(100) Tier, VARCHAR(100) LOB, CHAR(10) Gateway, BIGINT Day, BIGINT Hour, INTEGER Week, VARCHAR(100) Week_end_date, BIGINT Usage_Cnt): rowcount = 100.0, cumulative cost = {7.42124275972414E9 rows, 7.663067405383167E10 cpu, 0.0 io, 2.24645048816E10 network, 2.692766612982188E8 memory}, id = 129301

    public OpDefInfo(String plan) {
      Pattern p = Pattern.compile( "^(\\d+)-(\\d+)(\\s+)(\\w+)(?:\\((.*)\\))?\\s*:\\s*(.*)$" );
      Matcher m = p.matcher(plan);
      if (!m.matches()) {
        throw new IllegalStateException( "Could not parse plan: " + plan );
      }
      majorId = Integer.parseInt(m.group(1));
      stepId = Integer.parseInt(m.group(2));
      name = m.group(4);
      args = m.group(5);
      String tail = m.group(6);
      String indent = m.group(3);
      globalLevel = (indent.length() - 4) / 2;

      p = Pattern.compile("rowType = RecordType\\((.*)\\): (rowcount .*)");
      m = p.matcher(tail);
      if ( m.matches() ) {
        columns = parseCols(m.group(1));
        tail = m.group(2);
      }

      p = Pattern.compile( "rowcount = ([\\d.E]+), cumulative cost = \\{([\\d.E]+) rows, ([\\d.E]+) cpu, ([\\d.E]+) io, ([\\d.E]+) network, ([\\d.E]+) memory\\}, id = (\\d+)");
      m = p.matcher(tail);
      if (! m.matches()) {
        throw new IllegalStateException("Could not parse costs: " + tail );
      }
      estRows = Double.parseDouble(m.group(1));
      estRowCost = Double.parseDouble(m.group(2));
      estCpuCost = Double.parseDouble(m.group(3));
      estIOCost = Double.parseDouble(m.group(4));
      estNetCost = Double.parseDouble(m.group(5));
      estMemoryCost = Double.parseDouble(m.group(6));
      id = Integer.parseInt(m.group(7));
    }

    public void printTree(String indent) {
      new TreePrinter().visit(this);
    }

    public OpDefInfo(int major, int id) {
      majorId = major;
      stepId = id;
    }

    @Override
    public String toString() {
      String head = "[OpDefInfo " + majorId + "-" + stepId + ": " + name;
      if ( isInferred ) {
        head += " (" + opName + ")";
      }
      return head + "]";
    }
  }

  /**
   * Visit a tree of operator definitions to support printing,
   * analysis and other tasks.
   */

  public static class TreeVisitor
  {
    public void visit(OpDefInfo root) {
      visit(root, 0);
    }
    public void visit(OpDefInfo node, int indent) {
      visitOp( node, indent );
      if (node.children.isEmpty()) {
        return;
      }
      if ( node.children.size() == 1) {
        visit(node.children.get(0), indent);
        return;
      }
      indent++;
      int i = 0;
      for (OpDefInfo child : node.children) {
        visitSubtree(node, i++, indent);
        visit(child, indent+1);
      }
    }

    protected void visitOp(OpDefInfo node, int indent) {
    }

    protected void visitSubtree(OpDefInfo node, int i, int indent) {
    }

    public String indentString(int indent, String pad) {
      StringBuilder buf = new StringBuilder();
      for (int i = 0; i < indent; i++) {
        buf.append( pad );
      }
      return buf.toString();
    }

    public String indentString(int indent) {
      return indentString(indent, "  ");
    }

    public String subtreeLabel(OpDefInfo node, int branch) {
      if (node.name.equals("HashJoin")) {
        return (branch == 0) ? "Probe" : "Build";
      } else {
        return "Input " + (branch + 1);
      }
    }
  }

  /**
   * Print the operator tree for analysis.
   */

  public static class TreePrinter extends TreeVisitor
  {
    @Override
    protected void visitOp(OpDefInfo node, int indent) {
      System.out.print( indentString(indent) );
      System.out.println( node.toString() );
    }

    @Override
    protected void visitSubtree(OpDefInfo node, int i, int indent) {
      System.out.print( indentString(indent) );
      System.out.println(subtreeLabel(node, i));
    }
  }

  /**
   * Print out the tree showing a comparison of estimated vs.
   * actual costs.
   */

  public static class CostPrinter extends TreeVisitor
  {
    @Override
    protected void visitOp(OpDefInfo node, int indentLevel) {
      System.out.print(String.format("%02d-%02d ", node.majorId, node.stepId));
      String indent = indentString(indentLevel, ". ");
      System.out.print( indent + node.name );
      if (node.opName != null) {
        System.out.print( " (" + node.opName + ")" );
      }
      System.out.println( );
      indent = indentString(15);
      System.out.print( indent );
      System.out.println(String.format("  Estimate: %,15.0f rows, %,7.0f MB",
                         node.estRows, node.estMemoryCost / 1024 / 1024) );
      System.out.print( indent );
      System.out.println(String.format("  Actual:   %,15d rows, %,7d MB",
                         node.actualRows, node.actualMemory / 1024 / 1024));
    }

    @Override
    protected void visitSubtree(OpDefInfo node, int i, int indent) {
      System.out.print( indentString(indent) + "      " );
      System.out.println(subtreeLabel(node, i));
    }
  }

  public Map<Integer,OperatorProfile> getOpInfo( ) {
    Map<Integer,String> ops = getOperators( );
    Map<Integer,OperatorProfile> info = new HashMap<>( );
    JsonArray frags = getFragmentProfile( );
    JsonObject fragProfile = frags.getJsonObject(0).getJsonArray("minorFragmentProfile").getJsonObject(0);
    JsonArray opList = fragProfile.getJsonArray("operatorProfile");
    for ( JsonObject opProfile : opList.getValuesAs(JsonObject.class) ) {
      parseOpProfile( ops, info, opProfile );
    }
    return info;
  }

  public List<OperatorProfile> getOpsOfType(int type) {
    List<OperatorProfile> ops = new ArrayList<>();
    Map<Integer,OperatorProfile> opMap = getOpInfo();
    for (OperatorProfile op : opMap.values()) {
      if (op.type == type) {
        ops.add(op);
      }
    }
    return ops;
  }

  private void parseOpProfile(Map<Integer, String> ops,
      Map<Integer, OperatorProfile> info, JsonObject opProfile) {
    OperatorProfile opInfo = new OperatorProfile( 0, 0, opProfile );
    opInfo.name = ops.get(opInfo.opId);
    info.put(opInfo.opId, opInfo);
  }

  public void printPlan() {
    new CostPrinter().visit( topoOrder.get(0) );
  }

  public void print() {
    Map<Integer, OperatorProfile> opInfo = getOpInfo();
    int n = opInfo.size();
    long totalSetup = 0;
    long totalProcess = 0;
    for ( int i = 0;  i <= n;  i++ ) {
      OperatorProfile op = opInfo.get(i);
      if ( op == null ) { continue; }
      totalSetup += op.setupMs;
      totalProcess += op.processMs;
    }
    long total = totalSetup + totalProcess;
    for ( int i = 0;  i <= n;  i++ ) {
      OperatorProfile op = opInfo.get(i);
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

  public List<OpDefInfo> getOpDefn(String target) {
    List<OpDefInfo> ops = new ArrayList<>( );
    for ( OpDefInfo opDef : operations ) {
      if ( opDef.name.startsWith( target ) ) {
        ops.add( opDef );
      }
    }
    return ops;
  }
}
