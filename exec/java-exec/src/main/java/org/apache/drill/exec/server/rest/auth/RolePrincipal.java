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
package org.apache.drill.exec.server.rest.auth;

import java.io.Serializable;
import java.security.Principal;

/**
 * Role principal implementation for Jetty 11+.
 * Replaced the removed RolePrincipal from AbstractLoginService.
 */
public class RolePrincipal implements Principal, Serializable {
  private static final long serialVersionUID = 1L;

  private final String _roleName;

  public RolePrincipal(String roleName) {
    _roleName = roleName;
  }

  @Override
  public String getName() {
    return _roleName;
  }

  @Override
  public int hashCode() {
    return _roleName.hashCode();
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof RolePrincipal && _roleName.equals(((RolePrincipal) o)._roleName);
  }

  @Override
  public String toString() {
    return "RolePrincipal[" + _roleName + "]";
  }
}
