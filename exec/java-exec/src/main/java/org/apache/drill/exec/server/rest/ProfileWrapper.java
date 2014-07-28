/**
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
package org.apache.drill.exec.server.rest;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.drill.exec.proto.UserBitShared.CoreOperatorType;
import org.apache.drill.exec.proto.UserBitShared.MajorFragmentProfile;
import org.apache.drill.exec.proto.UserBitShared.MinorFragmentProfile;
import org.apache.drill.exec.proto.UserBitShared.OperatorProfile;
import org.apache.drill.exec.proto.UserBitShared.QueryProfile;
import org.apache.drill.exec.proto.UserBitShared.StreamProfile;
import org.apache.drill.exec.proto.helper.QueryIdHelper;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Collections2;
import org.apache.drill.exec.proto.helper.QueryIdHelper;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.text.DateFormat;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class ProfileWrapper {
  public QueryProfile profile;
  public String id;

  public ProfileWrapper(QueryProfile profile) {
    this.profile = profile;
    this.id = QueryIdHelper.getQueryId(profile.getId());
  }

  public QueryProfile getProfile() {
    return profile;
  }

  public String getId() {
    return id;
  }

  public String getQueryId() {
    return QueryIdHelper.getQueryId(profile.getId());
  }
  
  public List<OperatorWrapper> getOperatorProfiles() {
    List<OperatorWrapper> ows = Lists.newArrayList();
    Map<ImmutablePair<Integer, Integer>, List<ImmutablePair<OperatorProfile, Integer>>> opmap = Maps.newHashMap();
    
    List<MajorFragmentProfile> majors = new ArrayList<>(profile.getFragmentProfileList());
    Collections.sort(majors, Comparators.majorIdCompare);
    for (MajorFragmentProfile major : majors) {
      
      List<MinorFragmentProfile> minors = new ArrayList<>(major.getMinorFragmentProfileList());
      Collections.sort(minors, Comparators.minorIdCompare);
      for (MinorFragmentProfile minor : minors) {
        
        List<OperatorProfile> ops = new ArrayList<>(minor.getOperatorProfileList());
        Collections.sort(ops, Comparators.operatorIdCompare);
        for (OperatorProfile op : ops) {
          
          ImmutablePair<Integer, Integer> ip = new ImmutablePair<>(
              major.getMajorFragmentId(), op.getOperatorId());
          if (!opmap.containsKey(ip)) {
            List<ImmutablePair<OperatorProfile, Integer>> l = Lists.newArrayList();
            opmap.put(ip, l);
          }
          opmap.get(ip).add(new ImmutablePair<>(op, minor.getMinorFragmentId()));
        }
      }
    }
    
    List<ImmutablePair<Integer, Integer>> keys = new ArrayList<>(opmap.keySet());
    Collections.sort(keys);
    for (ImmutablePair<Integer, Integer> ip : keys) {
      ows.add(new OperatorWrapper(ip.getLeft(), opmap.get(ip)));
    }
    
    return ows;
  }
  
  public List<FragmentWrapper> getFragmentProfiles() {
    List<FragmentWrapper> fws = Lists.newArrayList();
    
    List<MajorFragmentProfile> majors = new ArrayList<>(profile.getFragmentProfileList());
    Collections.sort(majors, Comparators.majorIdCompare);
    for (MajorFragmentProfile major : majors) {
      fws.add(new FragmentWrapper(major));
    }
    
    return fws;
  }

  public String getFragmentsOverview() {
    final String[] columns = {"Fragment", "Minor Fragments Reporting", "First Start", "Last Start", "First End", "Last End", "tmin", "tavg", "tmax"};
    TableBuilder tb = new TableBuilder(columns);
    for (FragmentWrapper fw : getFragmentProfiles()) {
      fw.addSummary(tb);
    }
    return tb.toString();
  }

  public String majorFragmentTimingProfile(MajorFragmentProfile majorFragmentProfile) {
    final String[] columns = {"Minor", "Start", "End", "Total Time", "Max Records", "Max Batches"};
    TableBuilder builder = new TableBuilder(columns);

    ArrayList<MinorFragmentProfile> complete, incomplete;
    complete = new ArrayList<MinorFragmentProfile>(Collections2.filter(
        majorFragmentProfile.getMinorFragmentProfileList(), Filters.hasOperatorsAndTimes));
    incomplete = new ArrayList<MinorFragmentProfile>(Collections2.filter(
        majorFragmentProfile.getMinorFragmentProfileList(), Filters.missingOperatorsOrTimes));

    Collections.sort(complete, Comparators.minorIdCompare);
    for (MinorFragmentProfile m : complete) {
      ArrayList<OperatorProfile> ops = new ArrayList<OperatorProfile>(m.getOperatorProfileList());

      long t0 = profile.getStart();
      long biggestIncomingRecords = 0;
      long biggestBatches = 0;

      for (OperatorProfile op : ops) {
        long incomingRecords = 0;
        long batches = 0;
        for (StreamProfile sp : op.getInputProfileList()) {
          incomingRecords += sp.getRecords();
          batches += sp.getBatches();
        }
        biggestIncomingRecords = Math.max(biggestIncomingRecords, incomingRecords);
        biggestBatches = Math.max(biggestBatches, batches);
      }

      builder.appendCell(String.format("#%d - %d", majorFragmentProfile.getMajorFragmentId(), m.getMinorFragmentId()), null);
      builder.appendMillis(m.getStartTime() - t0, null);
      builder.appendMillis(m.getEndTime() - t0, null);
      builder.appendMillis(m.getEndTime() - m.getStartTime(), null);
      
      builder.appendInteger(biggestIncomingRecords, null);
      builder.appendInteger(biggestBatches, null);
    }
    for (MinorFragmentProfile m : incomplete) {
      builder.appendCell(
          majorFragmentProfile.getMajorFragmentId() + "-"
              + m.getMinorFragmentId(), null);
      builder.appendRepeated(m.getState().toString(), null, 5);
    }
    return builder.toString();
  }
  
  public String getOperatorsOverview() {
    final String [] columns = {"Operator", "Type", "Setup (min)", "Setup (avg)", "Setup (max)", "Process (min)", "Process (avg)", "Process (max)", "Wait (min)", "Wait (avg)", "Wait (max)"};
    TableBuilder tb = new TableBuilder(columns);
    for (OperatorWrapper ow : getOperatorProfiles()) {
      ow.addSummary(tb);
    }
    return tb.toString();
  }
  
  public String getOperatorsJSON() {
    StringBuilder sb = new StringBuilder("{");
    String sep = "";
    for (CoreOperatorType op : CoreOperatorType.values()) {
      sb.append(String.format("%s\"%d\" : \"%s\"", sep, op.ordinal(), op));
      sep = ", ";
    }
    return sb.append("}").toString();
  }
  
  public class FragmentWrapper {
    MajorFragmentProfile major;
    public FragmentWrapper(MajorFragmentProfile major) {
      this.major = major;
    }
    
    public String getDisplayName() {
      return "Fragment #" + major.getMajorFragmentId();
    }
    
    public String getId() {
      return "fragment-" + major.getMajorFragmentId();
    }
    
    public void addSummary(TableBuilder tb) {
      final String fmt = " (%d)";
      long t0 = profile.getStart();
      
      ArrayList<MinorFragmentProfile> complete = new ArrayList<MinorFragmentProfile>(
          Collections2.filter(major.getMinorFragmentProfileList(), Filters.hasOperatorsAndTimes));

      tb.appendCell("#" + major.getMajorFragmentId(), null);
      tb.appendCell(complete.size() + " / " + major.getMinorFragmentProfileCount(), null);
      
      if (complete.size() < 1) {
        tb.appendRepeated("", null, 7);
        return;
      }

      int li = complete.size() - 1;

      Collections.sort(complete, Comparators.startTimeCompare);
      tb.appendMillis(complete.get(0).getStartTime() - t0, String.format(fmt, complete.get(0).getMinorFragmentId()));
      tb.appendMillis(complete.get(li).getStartTime() - t0, String.format(fmt, complete.get(li).getMinorFragmentId()));

      Collections.sort(complete, Comparators.endTimeCompare);
      tb.appendMillis(complete.get(0).getEndTime() - t0, String.format(fmt, complete.get(0).getMinorFragmentId()));
      tb.appendMillis(complete.get(li).getEndTime() - t0, String.format(fmt, complete.get(li).getMinorFragmentId()));
      
      long total = 0;
      for (MinorFragmentProfile p : complete) {
        total += p.getEndTime() - p.getStartTime();
      }
      Collections.sort(complete, Comparators.runTimeCompare);
      tb.appendMillis(complete.get(0).getEndTime() - complete.get(0).getStartTime(),
          String.format(fmt, complete.get(0).getMinorFragmentId()));
      tb.appendMillis((long) (total / complete.size()), null);
      tb.appendMillis(complete.get(li).getEndTime() - complete.get(li).getStartTime(),
          String.format(fmt, complete.get(li).getMinorFragmentId()));
    }
    
    public String getContent() {
      return majorFragmentTimingProfile(major);
    }
  }
  
  public class OperatorWrapper {
    int major;
    List<ImmutablePair<OperatorProfile, Integer>> ops;
    
    public OperatorWrapper(int major, List<ImmutablePair<OperatorProfile, Integer>> ops) {
      assert ops.size() > 0;
      this.major = major;
      this.ops = ops;
    }
    
    public String getDisplayName() {
      return String.format("Fragment #%d - Operator %d (%s)",
          major, ops.get(0).getLeft().getOperatorId(),
          CoreOperatorType.valueOf(ops.get(0).getLeft().getOperatorType() ).toString());
    }
    
    public String getId() {
      return String.format("operator-%d-%d", major, ops.get(0).getLeft().getOperatorId());
    }
    
    public String getContent() {
      final String [] columns = {"Fragment", "Setup", "Process", "Wait", "Max Batches", "Max Records"};
      TableBuilder builder = new TableBuilder(columns);
      
      for (ImmutablePair<OperatorProfile, Integer> ip : ops) {
        int minor = ip.getRight();
        OperatorProfile op = ip.getLeft();
        
        builder.appendCell(String.format("#%d - %d", major, minor), null);
        builder.appendNanos(op.getSetupNanos(), null);
        builder.appendNanos(op.getProcessNanos(), null);
        builder.appendNanos(op.getWaitNanos(), null);
        
        long maxBatches = Long.MIN_VALUE;
        long maxRecords = Long.MIN_VALUE;
        for (StreamProfile sp : op.getInputProfileList()) {
          maxBatches = Math.max(sp.getBatches(), maxBatches);
          maxRecords = Math.max(sp.getRecords(), maxRecords);
        }
        
        builder.appendInteger(maxBatches, null);
        builder.appendInteger(maxRecords, null);
      }
      return builder.toString();
    }
    
    public void addSummary(TableBuilder tb) {
      tb.appendCell(String.format("#%d - Op %d", major, ops.get(0).getLeft().getOperatorId()), null);
      tb.appendCell(CoreOperatorType.valueOf(ops.get(0).getLeft().getOperatorType() ).toString(), null);

      int li = ops.size() - 1;
      String fmt = " (%s)";
      
      double setupSum = 0.0;
      double processSum = 0.0;
      double waitSum = 0.0;
      for (ImmutablePair<OperatorProfile, Integer> ip : ops) {
        setupSum += ip.getLeft().getSetupNanos();
        processSum += ip.getLeft().getProcessNanos();
        waitSum += ip.getLeft().getWaitNanos();
      }
      
      Collections.sort(ops, Comparators.setupTimeSort);
      tb.appendNanos(ops.get(0).getLeft().getSetupNanos(), String.format(fmt, ops.get(0).getRight()));
      tb.appendNanos((long) (setupSum / ops.size()), null);
      tb.appendNanos(ops.get(li).getLeft().getSetupNanos(), String.format(fmt, ops.get(li).getRight()));
      
      Collections.sort(ops, Comparators.processTimeSort);
      tb.appendNanos(ops.get(0).getLeft().getProcessNanos(), String.format(fmt, ops.get(0).getRight()));
      tb.appendNanos((long) (processSum / ops.size()), null);
      tb.appendNanos(ops.get(li).getLeft().getProcessNanos(), String.format(fmt, ops.get(li).getRight()));
      
      Collections.sort(ops, Comparators.waitTimeSort);
      tb.appendNanos(ops.get(0).getLeft().getWaitNanos(), String.format(fmt, ops.get(0).getRight()));
      tb.appendNanos((long) (waitSum / ops.size()), null);
      tb.appendNanos(ops.get(li).getLeft().getWaitNanos(), String.format(fmt, ops.get(li).getRight()));
    }
  }

  static class Comparators {
    final static Comparator<MajorFragmentProfile> majorIdCompare = new Comparator<MajorFragmentProfile>() {
      public int compare(MajorFragmentProfile o1, MajorFragmentProfile o2) {
        return Long.compare(o1.getMajorFragmentId(), o2.getMajorFragmentId());
      }
    };
    
    final static Comparator<MinorFragmentProfile> minorIdCompare = new Comparator<MinorFragmentProfile>() {
      public int compare(MinorFragmentProfile o1, MinorFragmentProfile o2) {
        return Long.compare(o1.getMinorFragmentId(), o2.getMinorFragmentId());
      }
    };
    
    final static Comparator<MinorFragmentProfile> startTimeCompare = new Comparator<MinorFragmentProfile>() {
      public int compare(MinorFragmentProfile o1, MinorFragmentProfile o2) {
        return Long.compare(o1.getStartTime(), o2.getStartTime());
      }
    };

    final static Comparator<MinorFragmentProfile> endTimeCompare = new Comparator<MinorFragmentProfile>() {
      public int compare(MinorFragmentProfile o1, MinorFragmentProfile o2) {
        return Long.compare(o1.getEndTime(), o2.getEndTime());
      }
    };

    final static Comparator<MinorFragmentProfile> runTimeCompare = new Comparator<MinorFragmentProfile>() {
      public int compare(MinorFragmentProfile o1, MinorFragmentProfile o2) {
        return Long.compare(o1.getEndTime() - o1.getStartTime(), o2.getEndTime() - o2.getStartTime());
      }
    };
    
    final static Comparator<OperatorProfile> operatorIdCompare = new Comparator<OperatorProfile>() {
      public int compare(OperatorProfile o1, OperatorProfile o2) {
        return Long.compare(o1.getOperatorId(), o2.getOperatorId());
      }
    };
    
    final static Comparator<Pair<OperatorProfile, Integer>> setupTimeSort = new Comparator<Pair<OperatorProfile, Integer>>() {
      public int compare(Pair<OperatorProfile, Integer> o1, Pair<OperatorProfile, Integer> o2) {
        return Long.compare(o1.getLeft().getSetupNanos(), o2.getLeft().getSetupNanos());
      }
    };
    
    final static Comparator<Pair<OperatorProfile, Integer>> processTimeSort = new Comparator<Pair<OperatorProfile, Integer>>() {
      public int compare(Pair<OperatorProfile, Integer> o1, Pair<OperatorProfile, Integer> o2) {
        return Long.compare(o1.getLeft().getProcessNanos(), o2.getLeft().getProcessNanos());
      }
    };
    
    final static Comparator<Pair<OperatorProfile, Integer>> waitTimeSort = new Comparator<Pair<OperatorProfile, Integer>>() {
      public int compare(Pair<OperatorProfile, Integer> o1, Pair<OperatorProfile, Integer> o2) {
        return Long.compare(o1.getLeft().getWaitNanos(), o2.getLeft().getWaitNanos());
      }
    };
  }

  private static class Filters {
    final static Predicate<MinorFragmentProfile> hasOperators = new Predicate<MinorFragmentProfile>() {
      public boolean apply(MinorFragmentProfile arg0) {
        return arg0.getOperatorProfileCount() != 0;
      }
    };

    final static Predicate<MinorFragmentProfile> hasTimes = new Predicate<MinorFragmentProfile>() {
      public boolean apply(MinorFragmentProfile arg0) {
        return arg0.hasStartTime() && arg0.hasEndTime();
      }
    };

    final static Predicate<MinorFragmentProfile> hasOperatorsAndTimes = Predicates.and(Filters.hasOperators, Filters.hasTimes);

    final static Predicate<MinorFragmentProfile> missingOperatorsOrTimes = Predicates.not(hasOperatorsAndTimes);
  }
  
  class TableBuilder {
    NumberFormat format = NumberFormat.getInstance(Locale.US);
    DateFormat dateFormat = new SimpleDateFormat("HH:mm:ss.SSS");
    
    StringBuilder sb;
    int w = 0;
    int width;
    
    public TableBuilder(String[] columns) {
      sb = new StringBuilder();
      width = columns.length;
      
      format.setMaximumFractionDigits(3);
      format.setMinimumFractionDigits(3);
      
      sb.append("<table class=\"table table-bordered text-right\">\n<tr>");
      for (String cn : columns) {
        sb.append("<th>" + cn + "</th>");
      }
      sb.append("</tr>\n");
    }
    
    public void appendCell(String s, String link) {
      if (w == 0) {
        sb.append("<tr>");
      }
      sb.append(String.format("<td>%s%s</td>", s, link != null ? link : ""));
      if (++w >= width) {
        sb.append("</tr>\n");
        w = 0;
      }
    }
    
    public void appendRepeated(String s, String link, int n) {
      for (int i = 0; i < n; i++) {
        appendCell(s, link);
      }
    }

    public void appendTime(long d, String link) {
      appendCell(dateFormat.format(d), link);
    }
    
    public void appendMillis(long p, String link) {
      appendCell(format.format(p / 1000.0), link);
    }
    
    public void appendNanos(long p, String link) {
      appendMillis((long) (p / 1000.0 / 1000.0), link);
    }
    
    public void appendFormattedNumber(Number n, String link) {
      appendCell(format.format(n), link);
    }

    public void appendInteger(long l, String link) {
      appendCell(Long.toString(l), link);
    }
    
    public String toString() {
      String rv;
      rv = sb.append("\n</table>").toString();
      sb = null;
      return rv;
    }
  }
}
