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

import java.util.Comparator;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.drill.exec.proto.UserBitShared.MajorFragmentProfile;
import org.apache.drill.exec.proto.UserBitShared.MinorFragmentProfile;
import org.apache.drill.exec.proto.UserBitShared.OperatorProfile;

interface Comparators {
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

  final static Comparator<MinorFragmentProfile> fragPeakMemAllocated = new Comparator<MinorFragmentProfile>() {
    public int compare(MinorFragmentProfile o1, MinorFragmentProfile o2) {
      return Long.compare(o1.getMaxMemoryUsed(), o2.getMaxMemoryUsed());
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

  final static Comparator<Pair<OperatorProfile, Integer>> opPeakMem = new Comparator<Pair<OperatorProfile, Integer>>() {
    public int compare(Pair<OperatorProfile, Integer> o1, Pair<OperatorProfile, Integer> o2) {
      return Long.compare(o1.getLeft().getPeakLocalMemoryAllocated(), o2.getLeft().getPeakLocalMemoryAllocated());
    }
  };
}
