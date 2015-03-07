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

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.store.AbstractRecordReader;

/**
 * A record reader to populate a {@link SystemRecord}.
 */
public class SystemRecordReader extends AbstractRecordReader {
//  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SystemRecordReader.class);

  private final FragmentContext fragmentContext;
  private final SystemRecord record;
  private boolean read;

  private OperatorContext operatorContext;

  public SystemRecordReader(FragmentContext context, SystemRecord record) {
    this.fragmentContext = context;
    this.record = record;
    this.read = false;
  }

  @Override
  public void setup(OutputMutator output) throws ExecutionSetupException {
    try {
      record.setup(output);
    } catch (SchemaChangeException e) {
      throw new ExecutionSetupException(e);
    }
  }

  @Override
  public void setOperatorContext(OperatorContext operatorContext) {
    this.operatorContext = operatorContext;
  }

  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public int next() {
    // send only one record
    if (!read) {
      record.setRecordValues(fragmentContext);
      read = true;
      return 1;
    }
    return 0;
  }

  @Override
  public void cleanup() {
  }
}
