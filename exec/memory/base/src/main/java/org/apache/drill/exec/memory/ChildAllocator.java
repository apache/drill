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
 * Child allocator class. Only slightly different from the {@see RootAllocator},
 * in that these can't be created directly, but must be obtained from
 * {@see BufferAllocator#newChildAllocator(AllocatorOwner, long, long, int)}.

 * <p>Child allocators can only be created by the root, or other children, so
 * this class is package private.</p>
 */
class ChildAllocator extends BaseAllocator {
  /**
   * Constructor.
   *
   * @param parentAllocator parent allocator -- the one creating this child
   * @param allocatorOwner a handle to the object making the request
   * @param allocationPolicy the allocation policy to use; the policy for all
   *   allocators must match for each invocation of a drillbit
   * @param initReservation initial amount of space to reserve (obtained from the parent)
   * @param maxAllocation maximum amount of space that can be obtained from this allocator;
   *   note this includes direct allocations (via {@see BufferAllocator#buffer(int, int)}
   *   et al) and requests from descendant allocators. Depending on the allocation policy in
   *   force, even less memory may be available
   * @param flags one or more of BaseAllocator.F_* flags
   */
  ChildAllocator(
      BaseAllocator parentAllocator,
      String name,
      long initReservation,
      long maxAllocation) {
    super(parentAllocator, name, initReservation, maxAllocation);
  }


}
