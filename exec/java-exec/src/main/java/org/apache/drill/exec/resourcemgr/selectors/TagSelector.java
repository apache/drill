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
package org.apache.drill.exec.resourcemgr.selectors;

import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.resourcemgr.exception.RMConfigException;

public class TagSelector extends AbstractResourcePoolSelector {

  private String configuredTag;

  TagSelector(String selectorValue) throws RMConfigException {
    super(SelectorType.TAG);

    if (selectorValue == null || selectorValue.isEmpty()) {
      throw new RMConfigException("Tag value of this selector is either null or empty. Please configure a valid tag " +
        "as string.");
    }
    configuredTag = selectorValue;
  }

  @Override
  public SelectorType getSelectorType() {
    return SELECTOR_TYPE;
  }

  @Override
  public boolean isQuerySelected(QueryContext queryContext) {
    String[] queryTags = queryContext.getOption(ExecConstants.RM_QUERY_TAGS_KEY).string_val.split(",");
    for (String queryTag : queryTags) {
      if (queryTag.equals(configuredTag)) {
        return true;
      }
    }
    return false;
  }

  public String getTagValue() {
    return configuredTag;
  }

  @Override
  public String toString() {
    return "{ SelectorType: " + super.toString() + ", TagValue: [" + configuredTag + "]}";
  }
}
