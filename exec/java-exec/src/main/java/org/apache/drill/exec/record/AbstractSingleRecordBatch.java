/*
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
package org.apache.drill.exec.record;

import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.base.PhysicalOperator;

/**
 * Implements an AbstractUnaryRecordBatch where the inoming record batch is known at the time of creation
 * @param <T>
 */
public abstract class AbstractSingleRecordBatch<T extends PhysicalOperator> extends AbstractUnaryRecordBatch<T> {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(new Object() {}.getClass().getEnclosingClass());

  protected final RecordBatch incoming;

  public AbstractSingleRecordBatch(T popConfig, FragmentContext context, RecordBatch incoming) throws OutOfMemoryException {
    super(popConfig, context);
    this.incoming = incoming;
  }

  @Override
  protected RecordBatch getIncoming() {
    return incoming;
  }

}
