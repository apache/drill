/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file distributed with
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
package org.apache.drill.exec.security.spi;

import java.security.Principal;
import java.util.Collections;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * The identity of a querying user, as seen by authorization plugins.
 * Carries the user name, optional group names (resolved by the engine at
 * authentication time) and an optional {@link Principal}.
 */
public final class UserIdentity {

  private final String user;
  private final Set<String> groups;
  private final Optional<Principal> principal;

  private UserIdentity(String user, Set<String> groups,
      Optional<Principal> principal) {
    this.user = Objects.requireNonNull(user, "user is null");
    this.groups = groups == null ? Collections.emptySet() : Collections.unmodifiableSet(groups);
    this.principal = principal == null ? Optional.empty() : principal;
  }

  public String getUser() {
    return user;
  }

  public Set<String> getGroups() {
    return groups;
  }

  public Optional<Principal> getPrincipal() {
    return principal;
  }

  // ---------- Builder ----------
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Convenience factory for a user identity with only a user name
   * (no groups, no principal). Used by engine mount points that only
   * have the authenticated user name available.
   */
  public static UserIdentity of(String user) {
    return builder().setUser(user).build();
  }

  public static class Builder {
    private String user;
    private Set<String> groups;
    private Optional<Principal> principal = Optional.empty();

    public Builder setUser(String user) {
      this.user = user;
      return this;
    }

    public Builder setGroups(Set<String> groups) {
      this.groups = groups;
      return this;
    }

    public Builder setPrincipal(Optional<Principal> principal) {
      this.principal = principal;
      return this;
    }

    public UserIdentity build() {
      return new UserIdentity(user, groups, principal);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    UserIdentity that = (UserIdentity) o;
    return user.equals(that.user) &&
        groups.equals(that.groups) &&
        principal.equals(that.principal);
  }

  @Override
  public int hashCode() {
    return Objects.hash(user, groups, principal);
  }

  @Override
  public String toString() {
    return "UserIdentity{user='" + user + "', groups=" + groups + "}";
  }
}
