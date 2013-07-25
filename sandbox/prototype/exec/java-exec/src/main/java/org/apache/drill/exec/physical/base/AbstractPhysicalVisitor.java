/*******************************************************************************
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
 ******************************************************************************/
package org.apache.drill.exec.physical.base;

import org.apache.drill.exec.physical.config.Filter;
import org.apache.drill.exec.physical.config.HashPartitionSender;
import org.apache.drill.exec.physical.config.HashToRandomExchange;
import org.apache.drill.exec.physical.config.Project;
import org.apache.drill.exec.physical.config.RandomReceiver;
import org.apache.drill.exec.physical.config.RangeSender;
import org.apache.drill.exec.physical.config.Screen;
import org.apache.drill.exec.physical.config.SingleSender;
import org.apache.drill.exec.physical.config.Sort;
import org.apache.drill.exec.physical.config.UnionExchange;

public abstract class AbstractPhysicalVisitor<T, X, E extends Throwable> implements PhysicalVisitor<T, X, E> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AbstractPhysicalVisitor.class);

  @Override
  public T visitExchange(Exchange exchange, X value) throws E{
    return visitOp(exchange, value);
  }

  @Override
  public T visitFilter(Filter filter, X value) throws E{
    return visitOp(filter, value);
  }

  @Override
  public T visitProject(Project project, X value) throws E{
    return visitOp(project, value);
  }

  @Override
  public T visitSort(Sort sort, X value) throws E{
    return visitOp(sort, value);
  }

  @Override
  public T visitSender(Sender sender, X value) throws E {
    return visitOp(sender, value);
  }

  @Override
  public T visitReceiver(Receiver receiver, X value) throws E {
    return visitOp(receiver, value);
  }

  @Override
  public T visitScan(Scan<?> scan, X value) throws E{
    return visitOp(scan, value);
  }

  @Override
  public T visitStore(Store store, X value) throws E{
    return visitOp(store, value);
  }

  
  public T visitChildren(PhysicalOperator op, X value) throws E{
    for(PhysicalOperator child : op){
      child.accept(this, value);
    }
    return null;
  }
  
  @Override
  public T visitHashPartitionSender(HashPartitionSender op, X value) throws E {
    return visitSender(op, value);
  }

  @Override
  public T visitRandomReceiver(RandomReceiver op, X value) throws E {
    return visitReceiver(op, value);
  }

  @Override
  public T visitHashPartitionSender(HashToRandomExchange op, X value) throws E {
    return visitExchange(op, value);
  }

  @Override
  public T visitRangeSender(RangeSender op, X value) throws E {
    return visitSender(op, value);
  }

  @Override
  public T visitScreen(Screen op, X value) throws E {
    return visitStore(op, value);
  }

  @Override
  public T visitSingleSender(SingleSender op, X value) throws E {
    return visitSender(op, value);
  }

  @Override
  public T visitUnionExchange(UnionExchange op, X value) throws E {
    return visitExchange(op, value);
  }

  @Override
  public T visitOp(PhysicalOperator op, X value) throws E{
    throw new UnsupportedOperationException(String.format(
        "The PhysicalVisitor of type %s does not currently support visiting the PhysicalOperator type %s.", this
            .getClass().getCanonicalName(), op.getClass().getCanonicalName()));
  }

}
