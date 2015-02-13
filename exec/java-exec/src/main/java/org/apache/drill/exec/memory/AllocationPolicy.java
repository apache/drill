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
package org.apache.drill.exec.memory;

/**
 * Implicitly specifies an allocation policy by providing a factory method to
 * create an enforcement agent.
 *
 * <p>Allocation policies are meant to be global, and may not work properly if
 * different allocators are given different policies. These are designed to
 * be supplied to the root-most allocator only, and then shared with descendant
 * (child) allocators.</p>
 */
public interface AllocationPolicy {
  /**
   * Create an allocation policy enforcement agent. Each newly created allocator should
   * call this in order to obtain its own agent.
   *
   * @return the newly instantiated agent; if an agent's implementation is stateless,
   *   this may return a sharable singleton
   */
  AllocationPolicyAgent newAgent();
}
