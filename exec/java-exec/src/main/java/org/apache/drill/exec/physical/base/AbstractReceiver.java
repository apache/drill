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
package org.apache.drill.exec.physical.base;

import java.util.Iterator;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;

public abstract class AbstractReceiver extends AbstractBase implements Receiver{

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AbstractReceiver.class);

  private final int oppositeMajorFragmentId;

  public AbstractReceiver(int oppositeMajorFragmentId){
    this.oppositeMajorFragmentId = oppositeMajorFragmentId;
  }

  @Override
  public Iterator<PhysicalOperator> iterator() {
    return Iterators.emptyIterator();
  }

  @Override
  public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E {
    return physicalVisitor.visitReceiver(this, value);
  }

  @Override
  public final PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) {
    Preconditions.checkArgument(children.isEmpty());
    //rewriting is unnecessary since the inputs haven't changed.
    return this;
  }

  @JsonProperty("sender-major-fragment")
  public int getOppositeMajorFragmentId() {
    return oppositeMajorFragmentId;
  }

}

