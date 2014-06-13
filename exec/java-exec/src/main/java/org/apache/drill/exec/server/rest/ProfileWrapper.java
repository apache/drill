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

import com.google.common.collect.Lists;
import org.apache.drill.exec.proto.UserBitShared.CoreOperatorType;
import org.apache.drill.exec.proto.UserBitShared.MajorFragmentProfile;
import org.apache.drill.exec.proto.UserBitShared.MinorFragmentProfile;
import org.apache.drill.exec.proto.UserBitShared.OperatorProfile;
import org.apache.drill.exec.proto.UserBitShared.QueryProfile;
import org.apache.drill.exec.proto.UserBitShared.StreamProfile;

import java.text.DateFormat;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Locale;

public class ProfileWrapper {

  NumberFormat format = NumberFormat.getInstance(Locale.US);
  DateFormat dateFormat = new SimpleDateFormat("HH:mm:ss.SSS");

  public QueryProfile profile;

  public ProfileWrapper(QueryProfile profile) {
    this.profile = profile;
  }

  public QueryProfile getProfile() {
    return profile;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("MAJOR FRAGMENTS\nid\tfirst start\tlast start\tfirst end\tlast end\tmin\tavg\tmax\t(time in ms)\n\n" + listMajorFragments());
    builder.append("\n");
    for (MajorFragmentProfile majorProfile : profile.getFragmentProfileList()) {
      builder.append(String.format("Major Fragment: %d\n%s\n", majorProfile.getMajorFragmentId(), printOperatorsInMajor(majorProfile)));
    }
    builder.append("\n");
    for (MajorFragmentProfile majorProfile : profile.getFragmentProfileList()) {
      builder.append(String.format("Major Fragment: %d\n%s\n", majorProfile.getMajorFragmentId(), printMinorFragmentsInMajor(majorProfile)));
    }
    return builder.toString();
  }

  public String listMajorFragments() {
    StringBuilder builder = new StringBuilder();
    for (MajorFragmentProfile m : profile.getFragmentProfileList()) {
      List<Long> totalTimes = Lists.newArrayList();
      List<Long> startTimes = Lists.newArrayList();
      List<Long> endTimes = Lists.newArrayList();
      for (MinorFragmentProfile minorFragmentProfile : m.getMinorFragmentProfileList()) {
        totalTimes.add(minorFragmentProfile.getEndTime() - minorFragmentProfile.getStartTime());
        startTimes.add(minorFragmentProfile.getStartTime());
        endTimes.add(minorFragmentProfile.getEndTime());
      }
      long min = Collections.min(totalTimes);
      long max = Collections.max(totalTimes);
      long sum = 0;
      for (Long l : totalTimes) {
        sum += l;
      }
      long firstStart = Collections.min(startTimes);
      long lastStart = Collections.max(startTimes);
      long firstEnd = Collections.min(endTimes);
      long lastEnd = Collections.max(endTimes);
      long avg = sum / totalTimes.size();
      builder.append(String.format("%d\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n", m.getMajorFragmentId(), dateFormat.format(new Date(firstStart)),
              dateFormat.format(new Date(lastStart)), dateFormat.format(new Date(firstEnd)), dateFormat.format(new Date(lastEnd)),
              format.format(min), format.format(avg), format.format(max)));
    }
    return builder.toString();
  }

  public String printMinorFragmentsInMajor(MajorFragmentProfile majorFragmentProfile) {
    StringBuilder builder = new StringBuilder();
    builder.append("id\tstart\tend\ttotal time (ms)\tmax records\tbatches\n");
    for (MinorFragmentProfile m : majorFragmentProfile.getMinorFragmentProfileList()) {
      long startTime = m.getStartTime();
      long endTime = m.getEndTime();

      List<OperatorProfile> operators = m.getOperatorProfileList();
      OperatorProfile biggest = null;
      int biggestIncomingRecords = 0;
      for (OperatorProfile oProfile : operators) {
        if (biggest == null) {
          biggest = oProfile;
          int incomingRecordCount = 0;
          for (StreamProfile streamProfile : oProfile.getInputProfileList()) {
            incomingRecordCount += streamProfile.getRecords();
          }
          biggestIncomingRecords = incomingRecordCount;
        } else {
          int incomingRecordCount = 0;
          for (StreamProfile streamProfile : oProfile.getInputProfileList()) {
            incomingRecordCount += streamProfile.getRecords();
          }
          if (incomingRecordCount > biggestIncomingRecords) {
            biggest = oProfile;
            biggestIncomingRecords = incomingRecordCount;
          }
        }
      }

      int biggestBatches = 0;
      for (StreamProfile sProfile : biggest.getInputProfileList()) {
        biggestBatches += sProfile.getBatches();
      }

      builder.append(String.format("%d\t%s\t%s\t%s\t%s\t%s\n", m.getMinorFragmentId(), dateFormat.format(new Date(startTime)),
              dateFormat.format(new Date(endTime)), format.format(endTime - startTime), biggestIncomingRecords, biggestBatches));
    }
    return builder.toString();
  }

  public String printOperatorsInMajor(MajorFragmentProfile majorFragmentProfile) {
    StringBuilder builder = new StringBuilder();
    builder.append("id\ttype\tp min\tp avg\tp max\ts min\ts avg\ts max\tw min\tw avg\tw max\n");
    int numOperators = majorFragmentProfile.getMinorFragmentProfile(0).getOperatorProfileCount();
    int numFragments = majorFragmentProfile.getMinorFragmentProfileCount();
    long[][] processing = new long[numOperators + 1][numFragments];
    long[][] setup = new long[numOperators + 1][numFragments];
    long[][] wait = new long[numOperators + 1][numFragments];
    CoreOperatorType[] operatorTypes = new CoreOperatorType[numOperators + 1];

    for (int i = 0; i < numFragments; i++) {
      MinorFragmentProfile minorProfile = majorFragmentProfile.getMinorFragmentProfile(i);
      for (int j = 0; j < numOperators; j++) {
        OperatorProfile operatorProfile = minorProfile.getOperatorProfile(j);
        int operatorId = operatorProfile.getOperatorId();
        processing[operatorId][i] = operatorProfile.getProcessNanos();
        setup[operatorId][i] = operatorProfile.getSetupNanos();
        wait[operatorId][i] = operatorProfile.getWaitNanos();
        if (i == 0) {
          operatorTypes[operatorId] = CoreOperatorType.valueOf(operatorProfile.getOperatorType());
        }
      }
    }

    for (int j = 0; j < numOperators + 1; j++) {
      if (operatorTypes[j] == null) {
        continue;
      }
      long processingMin = Long.MAX_VALUE;
      long processingMax = Long.MIN_VALUE;
      long processingSum = 0;
      long setupMin = Long.MAX_VALUE;
      long setupMax = Long.MIN_VALUE;
      long setupSum = 0;
      long waitMin = Long.MAX_VALUE;
      long waitMax = Long.MIN_VALUE;
      long waitSum = 0;

      for (int i = 0; i < numFragments; i++) {
        processingMin = Math.min(processingMin, processing[j][i]);
        processingMax = Math.max(processingMax, processing[j][i]);
        processingSum += processing[j][i];

        setupMin = Math.min(setupMin, setup[j][i]);
        setupMax = Math.max(setupMax, setup[j][i]);
        setupSum += setup[j][i];

        waitMin = Math.min(waitMin, wait[j][i]);
        waitMax = Math.max(waitMax, wait[j][i]);
        waitSum += wait[j][i];
      }

      long processingAvg = processingSum / numFragments;
      long setupAvg = setupSum / numFragments;
      long waitAvg = waitSum / numFragments;

      builder.append(String.format("%d\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n", j, operatorTypes[j].toString(),
              format.format(processingMin/1000/1000), format.format(processingAvg/1000/1000), format.format(processingMax/1000/1000),
              format.format(setupMin/1000/1000), format.format(setupAvg/1000/1000), format.format(setupMax/1000/1000),
              format.format(waitMin/1000/1000), format.format(waitAvg/1000/1000), format.format(waitMax/1000/1000)));
    }
    return builder.toString();
  }
}
