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
 * JMX bean for getting statistics about allocators.
 */
public interface RootAllocatorStatsMXBean {
  /**
   * Get the amount of memory this allocator owns. This includes its
   * allocated memory, and may include additional memory that it may hand
   * out without going to its parent for more.
   *
   * @return the amount of memory owned by this allocator
   */
  public long getOwnedMemory();

  /**
   * Get the amount of memory this allocator has allocated. This includes
   * buffers it has allocated and memory it has given to its children to manage.
   *
   * @return the amount of memory allocated by this allocator
   */
  public long getAllocatedMemory();

  /**
   * Get the number of child allocators this allocator owns.
   *
   * @return the number of child allocators this allocator owns
   */
  public long getChildCount();
}
