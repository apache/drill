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
package org.apache.drill.exec.server.rest.profile;

import java.util.ArrayList;
import java.util.Collections;

import org.apache.drill.exec.proto.UserBitShared.MajorFragmentProfile;
import org.apache.drill.exec.proto.UserBitShared.MinorFragmentProfile;
import org.apache.drill.exec.proto.UserBitShared.OperatorProfile;
import org.apache.drill.exec.proto.UserBitShared.StreamProfile;

import com.google.common.base.Preconditions;
import com.google.common.collect.Collections2;

public class FragmentWrapper {
  private final MajorFragmentProfile major;
  private final long start;

  public FragmentWrapper(MajorFragmentProfile major, long start) {
    this.major = Preconditions.checkNotNull(major);
    this.start = start;
  }

  public String getDisplayName() {
    return String.format("Major Fragment: %s", new OperatorPathBuilder().setMajor(major).build());
  }

  public String getId() {
    return String.format("fragment-%s", major.getMajorFragmentId());
  }

  public void addSummary(TableBuilder tb) {
    final String fmt = " (%d)";
    long t0 = start;

    ArrayList<MinorFragmentProfile> complete = new ArrayList<MinorFragmentProfile>(
        Collections2.filter(major.getMinorFragmentProfileList(), Filters.hasOperatorsAndTimes));

    tb.appendCell(new OperatorPathBuilder().setMajor(major).build(), null);
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

    Collections.sort(complete, Comparators.fragPeakMemAllocated);
    tb.appendBytes(complete.get(li).getMaxMemoryUsed(), null);
  }

  public String getContent() {
    return majorFragmentTimingProfile(major);
  }


  public String majorFragmentTimingProfile(MajorFragmentProfile major) {
    final String[] columns = {"Minor Fragment", "Host", "Start", "End", "Total Time", "Max Records", "Max Batches", "Peak Memory", "State"};
    TableBuilder builder = new TableBuilder(columns);

    ArrayList<MinorFragmentProfile> complete, incomplete;
    complete = new ArrayList<MinorFragmentProfile>(Collections2.filter(
        major.getMinorFragmentProfileList(), Filters.hasOperatorsAndTimes));
    incomplete = new ArrayList<MinorFragmentProfile>(Collections2.filter(
        major.getMinorFragmentProfileList(), Filters.missingOperatorsOrTimes));

    Collections.sort(complete, Comparators.minorIdCompare);
    for (MinorFragmentProfile minor : complete) {
      ArrayList<OperatorProfile> ops = new ArrayList<OperatorProfile>(minor.getOperatorProfileList());

      long t0 = start;
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

      builder.appendCell(new OperatorPathBuilder().setMajor(major).setMinor(minor).build(), null);
      builder.appendCell(minor.getEndpoint().getAddress(), null);
      builder.appendMillis(minor.getStartTime() - t0, null);
      builder.appendMillis(minor.getEndTime() - t0, null);
      builder.appendMillis(minor.getEndTime() - minor.getStartTime(), null);

      builder.appendFormattedInteger(biggestIncomingRecords, null);
      builder.appendFormattedInteger(biggestBatches, null);
      builder.appendBytes(minor.getMaxMemoryUsed(), null);
      builder.appendCell(minor.getState().name(), null);
    }
    for (MinorFragmentProfile m : incomplete) {
      builder.appendCell(
          major.getMajorFragmentId() + "-"
              + m.getMinorFragmentId(), null);
      builder.appendRepeated(m.getState().toString(), null, 6);
    }
    return builder.toString();
  }
}
