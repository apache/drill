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
package org.apache.drill.exec.store.druid.druid;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.collections.CollectionUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class PagingSpec {

  @JsonProperty
  private final boolean fromNext = true;

  @JsonProperty
  private final Map<String, Integer> pagingIdentifiers;

  @JsonProperty
  private int threshold;

  public PagingSpec(List<PagingIdentifier> pagingIdentifiers, int threshold) {
    this.pagingIdentifiers =
      CollectionUtils.isEmpty(pagingIdentifiers)
        ? new HashMap<>()
        : pagingIdentifiers.stream().collect(
        Collectors.toMap(PagingIdentifier::getSegmentName, PagingIdentifier::getSegmentOffset));
    this.threshold = threshold;
  }

  public Map<String, Integer> getPagingIdentifiers() {
    return pagingIdentifiers;
  }

  public int getThreshold() {
    return threshold;
  }

  public void setThreshold(int threshold) {
    this.threshold = threshold;
  }

  public boolean getFromNext() {
    return fromNext;
  }
}
