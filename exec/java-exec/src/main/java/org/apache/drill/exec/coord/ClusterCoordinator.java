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
package org.apache.drill.exec.coord;

import java.io.Closeable;
import java.util.Collection;

import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;

/**
 * Pluggable interface built to manage cluster coordination. Allows Drillbit or DrillClient to register its capabilities
 * as well as understand other node's existence and capabilities.
 **/
public abstract class ClusterCoordinator implements Closeable {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ClusterCoordinator.class);

  /**
   * Start the cluster coordinator.  Millis to wait is
   * @param millisToWait The maximum time to wait before throwing an exception if the cluster coordination service has not successfully started.  Use 0 to wait indefinitely.
   * @throws Exception
   */
  public abstract void start(long millisToWait) throws Exception;

  public abstract RegistrationHandle register(DrillbitEndpoint data);

  public abstract void unregister(RegistrationHandle handle);

  /**
   * Get a collection of avialable Drillbit endpoints, Thread-safe.
   * Could be slightly out of date depending on refresh policy.
   *
   * @return A collection of available endpoints.
   */
  public abstract Collection<DrillbitEndpoint> getAvailableEndpoints();

  public interface RegistrationHandle {
  }

  public abstract DistributedSemaphore getSemaphore(String name, int maximumLeases);


}
