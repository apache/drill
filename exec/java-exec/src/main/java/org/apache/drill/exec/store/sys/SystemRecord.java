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
package org.apache.drill.exec.store.sys;

import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.store.RecordDataType;

/**
 * A system record holds system information (e.g. memory usage).
 * Currently, there is only one such system record per Drillbit.
 */
public abstract class SystemRecord extends RecordDataType {

  /**
   * Setup value vectors to hold system information
   * @param output the mutator from {@link org.apache.drill.exec.store.sys.SystemRecordReader}
   * @throws SchemaChangeException
   */
  public abstract void setup(OutputMutator output) throws SchemaChangeException;

  /**
   * Set the values of value vectors when requested
   * @param context the context from {@link org.apache.drill.exec.store.sys.SystemRecordReader}
   */
  public abstract void setRecordValues(FragmentContext context);

}
