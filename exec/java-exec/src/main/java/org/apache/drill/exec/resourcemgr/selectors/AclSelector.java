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

import com.typesafe.config.Config;
import org.apache.drill.exec.ops.QueryContext;

import java.util.ArrayList;
import java.util.List;

public class AclSelector extends AbstractResourcePoolSelector {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AclSelector.class);

  private final List<String> allowedUsers = new ArrayList<>();

  private final List<String> allowedGroups = new ArrayList<>();

  private final List<String> disAllowedUsers = new ArrayList<>();

  private final List<String> disAllowedGroups = new ArrayList<>();

  private final Config aclSelectorValue;

  private static final String ACL_VALUE_GROUPS_KEY = "groups";

  private static final String ACL_VALUE_USERS_KEY = "users";

  private static final String ACL_LONG_SYNTAX_SEPARATOR = ":";

  private static final String ACL_LONG_ALLOWED_IDENTIFIER = "+";

  private static final String ACL_LONG_DISALLOWED_IDENTIFIER = "-";


  AclSelector(Config configValue) {
    super(SelectorType.ACL);
    this.aclSelectorValue = configValue;
    parseACLInput(aclSelectorValue.getStringList(ACL_VALUE_GROUPS_KEY), allowedGroups, disAllowedGroups);
    parseACLInput(aclSelectorValue.getStringList(ACL_VALUE_USERS_KEY), allowedUsers, disAllowedUsers);
  }

  // TODO: Implement this
  @Override
  public boolean isQuerySelected(QueryContext queryContext) {
    return false;
  }

  private void parseACLInput(List<String> acls, List<String> allowedIdentity, List<String> disAllowedIdentity) {
    for (String aclValue : acls) {

      if (aclValue.isEmpty()) {
        continue;
      }
      // Check if it's long form syntax or shortForm syntax
      String[] aclValueSplits = aclValue.split(ACL_LONG_SYNTAX_SEPARATOR);
      if (aclValueSplits.length == 1) {
        // short form
        allowedIdentity.add(aclValueSplits[0]);
      } else {
        // long form
        final String identifier = aclValueSplits[1];
        if (identifier.equals(ACL_LONG_ALLOWED_IDENTIFIER)) {
          allowedIdentity.add(aclValueSplits[0]);
        } else if (identifier.equals(ACL_LONG_DISALLOWED_IDENTIFIER)) {
          disAllowedIdentity.add(aclValueSplits[0]);
        } else {
          logger.error("Invalid long form syntax encountered hence ignoring ACL string {} . Details[Allowed identifiers " +
            "are `+` and `-`. Encountered: {}]", aclValue, identifier);
        }
      }
    }
  }
}
