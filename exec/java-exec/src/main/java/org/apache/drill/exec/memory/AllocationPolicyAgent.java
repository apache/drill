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
 * Per-allocator enforcement agent for allocation policies; created by
 * {@link AllocationPolicy#newAgent()}.
 */
public interface AllocationPolicyAgent extends AutoCloseable {
  /**
   * Checks to see if creating a new allocator using the given specifications
   * is allowed; should throw an exception if not.
   *
   * @param parentAllocator the parent allocator
   * @param initReservation initial reservation the allocator should have
   * @param maxAllocation the maximum allocation the allocator will allow
   * @param flags the allocation option flags
   * @throws OutOfMemoryException if the new allocator shouldn't be created
   */
  void checkNewAllocator(BufferAllocator parentAllocator,
      long initReservation, long maxAllocation, int flags);

  /**
   * Get the currently applicable memory limit for the provided allocator.
   * The interpretation of this value varies with the allocation policy in
   * use, and each policy should describe what to expect.
   *
   * @param bufferAllocator the allocator
   * @return the memory limit
   */
  long getMemoryLimit(BufferAllocator bufferAllocator);

  /**
   * Initialize the agent for a newly created allocator. Should be called from
   * the allocator's constructor to initialize the agent for the allocator.
   *
   * @param bufferAllocator the newly created allocator.
   */
  void initializeAllocator(BufferAllocator bufferAllocator);

  /**
   * Indicate if any available memory owned by this allocator should
   * be released to its parent. Allocators may use this to limit the
   * amount of unused memory they retain for future requests; agents may
   * request that memory be returned if there is currently a high demand
   * for memory that other allocators could use if this allocator
   * doesn't need it.
   *
   * @param bufferAllocator
   * @return true if available memory owned by this allocator should be given
   *   back to its parent
   */
  boolean shouldReleaseToParent(BufferAllocator bufferAllocator);
}
