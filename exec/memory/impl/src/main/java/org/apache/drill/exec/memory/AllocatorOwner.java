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

import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.testing.ExecutionControls;

/**
 * This interface provides a means for allocator owners to inject services
 * required by allocators, as well as to identify themselves for debugging purposes.
 * Identification is done by overriding the implementation of
 * {#link {@link Object#toString()}.
 */
public interface AllocatorOwner {
  /**
   * Get the current ExecutionControls from the allocator's owner.
   *
   * @return the current execution controls; may return null if this isn't
   *   possible
   */
  ExecutionControls getExecutionControls();

  @Deprecated // Only for TopLevelAllocator and its friends.
  FragmentContext getFragmentContext();
}
