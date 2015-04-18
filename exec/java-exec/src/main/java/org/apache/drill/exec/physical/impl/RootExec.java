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
package org.apache.drill.exec.physical.impl;

import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;

/**
 * A FragmentRoot is a node which is the last processing node in a query plan. FragmentTerminals include Exchange
 * output nodes and storage nodes.  They are there driving force behind the completion of a query.
 */
public interface RootExec extends AutoCloseable {
  /**
   * Do the next batch of work.
   * @return Whether or not additional batches of work are necessary.  False means that this fragment is done.
   */
  public boolean next();

  /**
   * Inform sender that receiving fragment is finished and doesn't need any more data
   * @param handle
   */
  public void receivingFragmentFinished(FragmentHandle handle);
}
