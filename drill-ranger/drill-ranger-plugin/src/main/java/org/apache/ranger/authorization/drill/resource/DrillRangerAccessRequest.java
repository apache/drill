/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ranger.authorization.drill.resource;

import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;


public class DrillRangerAccessRequest {

    private static final Logger LOG = LoggerFactory.getLogger(DrillRangerAccessRequest.class);

    private String user;
    private Set<String> groups = new HashSet<>();
    private DrillAccessResource resource;
    private DrillAccessType accessType;
    private String action;
    private String clientIPAddress;
    private String clientType;
    // Resource matching scope. Table-level checks use SELF_OR_DESCENDANTS so a
    // table request can match column-level policies (column is a descendant of
    // table in the resource hierarchy). Column-level checks use SELF for exact
    // column matching. Defaults to SELF_OR_DESCENDANTS to preserve historical
    // behavior when the caller does not specify a scope.
    private RangerAccessRequest.ResourceMatchingScope resourceMatchingScope =
        RangerAccessRequest.ResourceMatchingScope.SELF_OR_DESCENDANTS;

    private DrillRangerAccessRequest(Builder builder) {
        this.user = builder.user;
        this.groups = builder.groups;
        this.resource = builder.resource;
        this.accessType = builder.accessType;
        this.action = builder.action;
        this.clientIPAddress = builder.clientIPAddress;
        this.clientType = builder.clientType;
        this.resourceMatchingScope = builder.resourceMatchingScope;
    }

    public RangerAccessRequest toRangerRequest() {
        RangerAccessRequestImpl request = new RangerAccessRequestImpl();
        request.setUser(user);
        request.setUserGroups(groups);
        request.setResource(resource);
        // Access type name MUST match the service-def's accessTypes[].name exactly
        // (Ranger matching is case-sensitive). DrillAccessType enum constants are
        // uppercase (SELECT, CREATE, ...) and the service-def registers them as
        // uppercase too, so we use the enum name directly — no toLowerCase().
        request.setAccessType(accessType.name());
        request.setAction(action != null ? action : accessType.name());
        request.setClientIPAddress(clientIPAddress);
        request.setClientType(clientType);
        request.setResourceMatchingScope(resourceMatchingScope);

        return request;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String user;
        private Set<String> groups = new HashSet<>();
        private DrillAccessResource resource;
        private DrillAccessType accessType;
        private String action;
        private String clientIPAddress;
        private String clientType;
        private RangerAccessRequest.ResourceMatchingScope resourceMatchingScope =
            RangerAccessRequest.ResourceMatchingScope.SELF_OR_DESCENDANTS;

        public Builder user(String user) {
            this.user = user;
            return this;
        }

        public Builder groups(Set<String> groups) {
            this.groups = groups != null ? new HashSet<>(groups) : new HashSet<>();
            return this;
        }

        public Builder addGroup(String group) {
            this.groups.add(group);
            return this;
        }

        public Builder resource(DrillAccessResource resource) {
            this.resource = resource;
            return this;
        }

        public Builder accessType(DrillAccessType accessType) {
            this.accessType = accessType;
            return this;
        }

        public Builder action(String action) {
            this.action = action;
            return this;
        }

        public Builder clientIPAddress(String clientIPAddress) {
            this.clientIPAddress = clientIPAddress;
            return this;
        }

        public Builder clientType(String clientType) {
            this.clientType = clientType;
            return this;
        }

        /**
         * Sets the resource matching scope. Use {@code SELF_OR_DESCENDANTS} for
         * table-level checks (so a table request can match column-level policies
         * whose resource is a descendant of table), and {@code SELF} for exact
         * column-level matching.
         *
         * @param scope the resource matching scope
         * @return this builder
         */
        public Builder resourceMatchingScope(RangerAccessRequest.ResourceMatchingScope scope) {
            this.resourceMatchingScope = scope;
            return this;
        }

        public DrillRangerAccessRequest build() {
            return new DrillRangerAccessRequest(this);
        }
    }
}
