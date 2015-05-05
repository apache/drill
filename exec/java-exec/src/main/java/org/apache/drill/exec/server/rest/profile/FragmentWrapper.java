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

  public FragmentWrapper(final MajorFragmentProfile major, final long start) {
    this.major = Preconditions.checkNotNull(major);
    this.start = start;
  }

  public String getDisplayName() {
    return String.format("Major Fragment: %s", new OperatorPathBuilder().setMajor(major).build());
  }

  public String getId() {
    return String.format("fragment-%s", major.getMajorFragmentId());
  }

  public static final String[] FRAGMENT_OVERVIEW_COLUMNS = {"Major Fragment", "Minor Fragments Reporting",
    "First Start", "Last Start", "First End", "Last End", "Min Runtime", "Avg Runtime", "Max Runtime", "Last Update",
    "Last Progress", "Max Peak Memory"};

  // Not including Major Fragment ID and Minor Fragments Reporting
  public static final int NUM_NULLABLE_OVERVIEW_COLUMNS = FRAGMENT_OVERVIEW_COLUMNS.length - 2;

  public void addSummary(TableBuilder tb) {
    final String fmt = " (%d)";
    final long t0 = start;

    final ArrayList<MinorFragmentProfile> complete = new ArrayList<MinorFragmentProfile>(
        Collections2.filter(major.getMinorFragmentProfileList(), Filters.hasOperatorsAndTimes));

    tb.appendCell(new OperatorPathBuilder().setMajor(major).build(), null);
    tb.appendCell(complete.size() + " / " + major.getMinorFragmentProfileCount(), null);

    if (complete.size() < 1) {
      tb.appendRepeated("", null, NUM_NULLABLE_OVERVIEW_COLUMNS);
      return;
    }

    final MinorFragmentProfile firstStart = Collections.min(complete, Comparators.startTimeCompare);
    final MinorFragmentProfile lastStart = Collections.max(complete, Comparators.startTimeCompare);
    tb.appendMillis(firstStart.getStartTime() - t0, String.format(fmt, firstStart.getMinorFragmentId()));
    tb.appendMillis(lastStart.getStartTime() - t0, String.format(fmt, lastStart.getMinorFragmentId()));

    final MinorFragmentProfile firstEnd = Collections.min(complete, Comparators.endTimeCompare);
    final MinorFragmentProfile lastEnd = Collections.max(complete, Comparators.endTimeCompare);
    tb.appendMillis(firstEnd.getEndTime() - t0, String.format(fmt, firstEnd.getMinorFragmentId()));
    tb.appendMillis(lastEnd.getEndTime() - t0, String.format(fmt, lastEnd.getMinorFragmentId()));

    long total = 0;
    for (final MinorFragmentProfile p : complete) {
      total += p.getEndTime() - p.getStartTime();
    }

    final MinorFragmentProfile shortRun = Collections.min(complete, Comparators.endTimeCompare);
    final MinorFragmentProfile longRun = Collections.max(complete, Comparators.endTimeCompare);

    tb.appendMillis(shortRun.getEndTime() - shortRun.getStartTime(), String.format(fmt, shortRun.getMinorFragmentId()));
    tb.appendMillis((long) (total / complete.size()), null);
    tb.appendMillis(longRun.getEndTime() - longRun.getStartTime(), String.format(fmt, longRun.getMinorFragmentId()));

    final MinorFragmentProfile lastUpdate = Collections.max(complete, Comparators.lastUpdateCompare);
    tb.appendTime(lastUpdate.getLastUpdate(), null);

    final MinorFragmentProfile lastProgress = Collections.max(complete, Comparators.lastProgressCompare);
    tb.appendTime(lastProgress.getLastProgress(), null);

    final MinorFragmentProfile maxMem = Collections.max(complete, Comparators.fragPeakMemAllocated);
    tb.appendBytes(maxMem.getMaxMemoryUsed(), null);
  }

  public String getContent() {
    return majorFragmentTimingProfile(major);
  }

  public static final String[] FRAGMENT_COLUMNS = {"Minor Fragment ID", "Host Name", "Start", "End",
    "Runtime", "Max Records", "Max Batches", "Last Update", "Last Progress", "Peak Memory", "State"};

  // Not including minor fragment ID
  private static final int NUM_NULLABLE_FRAGMENTS_COLUMNS = FRAGMENT_COLUMNS.length - 1;

  public String majorFragmentTimingProfile(final MajorFragmentProfile major) {
    final TableBuilder builder = new TableBuilder(FRAGMENT_COLUMNS);

    ArrayList<MinorFragmentProfile> complete, incomplete;
    complete = new ArrayList<MinorFragmentProfile>(Collections2.filter(
        major.getMinorFragmentProfileList(), Filters.hasOperatorsAndTimes));
    incomplete = new ArrayList<MinorFragmentProfile>(Collections2.filter(
        major.getMinorFragmentProfileList(), Filters.missingOperatorsOrTimes));

    Collections.sort(complete, Comparators.minorIdCompare);
    for (final MinorFragmentProfile minor : complete) {
      final ArrayList<OperatorProfile> ops = new ArrayList<OperatorProfile>(minor.getOperatorProfileList());

      final long t0 = start;
      long biggestIncomingRecords = 0;
      long biggestBatches = 0;

      for (final OperatorProfile op : ops) {
        long incomingRecords = 0;
        long batches = 0;
        for (final StreamProfile sp : op.getInputProfileList()) {
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

      builder.appendTime(minor.getLastUpdate(), null);
      builder.appendTime(minor.getLastProgress(), null);
      builder.appendBytes(minor.getMaxMemoryUsed(), null);
      builder.appendCell(minor.getState().name(), null);
    }
    for (final MinorFragmentProfile m : incomplete) {
      builder.appendCell(
          major.getMajorFragmentId() + "-"
              + m.getMinorFragmentId(), null);
      builder.appendRepeated(m.getState().toString(), null, NUM_NULLABLE_FRAGMENTS_COLUMNS);
    }
    return builder.toString();
  }
}
