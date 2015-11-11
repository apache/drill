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
 * An {@link AllocatorOwner} implementation that supports linking ownership to
 * a parent object. This can be convenient for identification purposes, when the
 * parent of the allocator's owner is a better handle for debugging.
 *
 * <p>The implementation of {@link #getExecutionControls()} returns the childOwner's
 * response to getExecutionControls().</p>
 */
public class ChainedAllocatorOwner implements AllocatorOwner {
  private final AllocatorOwner childOwner;
  private final AllocatorOwner parentOwner;

  /**
   * Constructor.
   *
   * @param childOwner the owner of the allocator
   * @param parentOwner the object that owns or created the childOwner
   */
  public ChainedAllocatorOwner(AllocatorOwner childOwner, AllocatorOwner parentOwner) {
    this.childOwner = childOwner;
    this.parentOwner = parentOwner;
  }

  @Override
  public String toString() {
    return childOwner + "(owned by " + parentOwner + ')';
  }

  @Override
  public ExecutionControls getExecutionControls() {
    return childOwner.getExecutionControls();
  }

  @Override
  public FragmentContext getFragmentContext() {
    return childOwner.getFragmentContext();
  }
}
