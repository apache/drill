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

import java.text.NumberFormat;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

public class ProfileWrapper {

  NumberFormat format = NumberFormat.getInstance(Locale.US);

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
    builder.append("MAJOR FRAGMENTS\nid\tmin\tavg\tmax\t(time in ms)\n\n" + listMajorFragments());
    builder.append("\n");
    for (MajorFragmentProfile majorProfile : profile.getFragmentProfileList()) {
      builder.append(String.format("Major Fragment: %d\n%s\n", majorProfile.getMajorFragmentId(), new MajorFragmentWrapper(majorProfile).toString()));
    }
    return builder.toString();
  }

  public String listMajorFragments() {
    StringBuilder builder = new StringBuilder();
    for (MajorFragmentProfile m : profile.getFragmentProfileList()) {
      List<Long> totalTimes = Lists.newArrayList();
      for (MinorFragmentProfile minorFragmentProfile : m.getMinorFragmentProfileList()) {
        totalTimes.add(minorFragmentProfile.getEndTime() - minorFragmentProfile.getStartTime());
      }
      long min = Collections.min(totalTimes);
      long max = Collections.max(totalTimes);
      long sum = 0;
      for (Long l : totalTimes) {
        sum += l;
      }
      long avg = sum / totalTimes.size();
      builder.append(String.format("%d\t%s\t%s\t%s\n", m.getMajorFragmentId(), format.format(min), format.format(avg), format.format(max)));
    }
    return builder.toString();
  }

  public class MajorFragmentWrapper {
    MajorFragmentProfile majorFragmentProfile;

    public MajorFragmentWrapper(MajorFragmentProfile majorFragmentProfile) {
      this.majorFragmentProfile = majorFragmentProfile;
    }

    @Override
    public String toString() {
      return String.format("Minor Fragments\nid\ttotal time (ms)\n%s\nOperators\nid\ttype\tmin\tavg\tmax\t(time in ns)\n%s\n", new MinorFragmentsInMajor().toString(), new OperatorsInMajor().toString());
    }

    public class MinorFragmentsInMajor {

      @Override
      public String toString() {
        StringBuilder builder = new StringBuilder();
        for (MinorFragmentProfile minorFragmentProfile: majorFragmentProfile.getMinorFragmentProfileList()) {
          builder.append(String.format("%d\t%s\n", minorFragmentProfile.getMinorFragmentId(), format.format(minorFragmentProfile.getEndTime() - minorFragmentProfile.getStartTime())));
        }
        return builder.toString();
      }
    }

    public class OperatorsInMajor {

      @Override
      public String toString() {
        StringBuilder builder = new StringBuilder();
        int numOperators = majorFragmentProfile.getMinorFragmentProfile(0).getOperatorProfileCount();
        int numFragments = majorFragmentProfile.getMinorFragmentProfileCount();
        long[][] values = new long[numOperators + 1][numFragments];
        CoreOperatorType[] operatorTypes = new CoreOperatorType[numOperators + 1];

        for (int i = 0; i < numFragments; i++) {
          MinorFragmentProfile minorProfile = majorFragmentProfile.getMinorFragmentProfile(i);
          for (int j = 0; j < numOperators; j++) {
            OperatorProfile operatorProfile = minorProfile.getOperatorProfile(j);
            int operatorId = operatorProfile.getOperatorId();
            values[operatorId][i] = operatorProfile.getProcessNanos() + operatorProfile.getSetupNanos();
            if (i == 0) {
              operatorTypes[operatorId] = CoreOperatorType.valueOf(operatorProfile.getOperatorType());
            }
          }
        }

        for (int j = 0; j < numOperators + 1; j++) {
          if (operatorTypes[j] == null) {
            continue;
          }
          long min = Long.MAX_VALUE;
          long max = Long.MIN_VALUE;
          long sum = 0;

          for (int i = 0; i < numFragments; i++) {
            min = Math.min(min, values[j][i]);
            max = Math.max(max, values[j][i]);
            sum += values[j][i];
          }

          long avg = sum / numFragments;

          builder.append(String.format("%d\t%s\t%s\t%s\t%s\n", j, operatorTypes[j].toString(), format.format(min), format.format(avg), format.format(max)));
        }
        return builder.toString();
      }
    }
  }




}
