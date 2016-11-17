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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.drill.exec.proto.UserBitShared.CoreOperatorType;
import org.apache.drill.exec.proto.UserBitShared.MajorFragmentProfile;
import org.apache.drill.exec.proto.UserBitShared.MinorFragmentProfile;
import org.apache.drill.exec.proto.UserBitShared.OperatorProfile;
import org.apache.drill.exec.proto.UserBitShared.QueryProfile;
import org.apache.drill.exec.proto.helper.QueryIdHelper;
import org.apache.drill.exec.server.options.OptionList;
import org.apache.drill.exec.server.options.OptionValue;

import static com.fasterxml.jackson.databind.SerializationFeature.INDENT_OUTPUT;

/**
 * Wrapper class for a {@link #profile query profile}, so it to be presented through web UI.
 */
public class ProfileWrapper {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ProfileWrapper.class);
  private static final ObjectMapper mapper = new ObjectMapper().enable(INDENT_OUTPUT);

  private QueryProfile profile;
  private String id;
  private final List<FragmentWrapper> fragmentProfiles;
  private final List<OperatorWrapper> operatorProfiles;
  private OptionList options;

  public ProfileWrapper(final QueryProfile profile) {
    this.profile = profile;
    this.id = QueryIdHelper.getQueryId(profile.getId());

    final List<FragmentWrapper> fragmentProfiles = new ArrayList<>();

    final List<MajorFragmentProfile> majors = new ArrayList<>(profile.getFragmentProfileList());
    Collections.sort(majors, Comparators.majorId);

    for (final MajorFragmentProfile major : majors) {
      fragmentProfiles.add(new FragmentWrapper(major, profile.getStart()));
    }
    this.fragmentProfiles = fragmentProfiles;

    final List<OperatorWrapper> ows = new ArrayList<>();
    // temporary map to store (major_id, operator_id) -> [(op_profile, minor_id)]
    final Map<ImmutablePair<Integer, Integer>, List<ImmutablePair<OperatorProfile, Integer>>> opmap = new HashMap<>();

    Collections.sort(majors, Comparators.majorId);
    for (final MajorFragmentProfile major : majors) {

      final List<MinorFragmentProfile> minors = new ArrayList<>(major.getMinorFragmentProfileList());
      Collections.sort(minors, Comparators.minorId);
      for (final MinorFragmentProfile minor : minors) {

        final List<OperatorProfile> ops = new ArrayList<>(minor.getOperatorProfileList());
        Collections.sort(ops, Comparators.operatorId);
        for (final OperatorProfile op : ops) {

          final ImmutablePair<Integer, Integer> ip = new ImmutablePair<>(
              major.getMajorFragmentId(), op.getOperatorId());
          if (!opmap.containsKey(ip)) {
            final List<ImmutablePair<OperatorProfile, Integer>> l = new ArrayList<>();
            opmap.put(ip, l);
          }
          opmap.get(ip).add(new ImmutablePair<>(op, minor.getMinorFragmentId()));
        }
      }
    }

    final List<ImmutablePair<Integer, Integer>> keys = new ArrayList<>(opmap.keySet());
    Collections.sort(keys);

    for (final ImmutablePair<Integer, Integer> ip : keys) {
      ows.add(new OperatorWrapper(ip.getLeft(), opmap.get(ip)));
    }
    this.operatorProfiles = ows;

    try {
      options = mapper.readValue(profile.getOptionsJson(), OptionList.class);
    } catch (Exception e) {
      logger.error("Unable to deserialize query options", e);
      options = new OptionList();
    }
  }

  public boolean hasError() {
    return profile.hasError() && profile.getError() != null;
  }

  public QueryProfile getProfile() {
    return profile;
  }

  public String getQueryId() {
    return id;
  }

  public List<FragmentWrapper> getFragmentProfiles() {
    return fragmentProfiles;
  }

  public String getFragmentsOverview() {
    TableBuilder tb = new TableBuilder(FragmentWrapper.FRAGMENT_OVERVIEW_COLUMNS);
    for (final FragmentWrapper fw : fragmentProfiles) {
      fw.addSummary(tb);
    }
    return tb.build();
  }

  public List<OperatorWrapper> getOperatorProfiles() {
    return operatorProfiles;
  }

  public String getOperatorsOverview() {
    final TableBuilder tb = new TableBuilder(OperatorWrapper.OPERATORS_OVERVIEW_COLUMNS);
    for (final OperatorWrapper ow : operatorProfiles) {
      ow.addSummary(tb);
    }
    return tb.build();
  }

  public String getOperatorsJSON() {
    final StringBuilder sb = new StringBuilder("{");
    String sep = "";
    for (final CoreOperatorType op : CoreOperatorType.values()) {
      sb.append(String.format("%s\"%d\" : \"%s\"", sep, op.ordinal(), op));
      sep = ", ";
    }
    return sb.append("}").toString();
  }

  /**
   * Generates sorted map with properties used to display on Web UI,
   * where key is property name and value is property string value.
   * When property value is null, it would be replaced with 'null',
   * this is achieved using {@link String#valueOf(Object)} method.
   * Options will be stored in ascending key order, sorted according
   * to the natural order for the option name represented by {@link String}.
   *
   * @return map with properties names and string values
   */
  public Map<String, String> getOptions() {
    final Map<String, String> map = Maps.newTreeMap();
    for (OptionValue option : options) {
      map.put(option.getName(), String.valueOf(option.getValue()));
    }
    return map;
  }
}
