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

import org.apache.drill.exec.physical.config.BroadcastSender;
import org.apache.drill.exec.physical.config.Filter;
import org.apache.drill.exec.physical.config.HashAggregate;
import org.apache.drill.exec.physical.config.HashJoinPOP;
import org.apache.drill.exec.physical.config.HashPartitionSender;
import org.apache.drill.exec.physical.config.HashToRandomExchange;
import org.apache.drill.exec.physical.config.IteratorValidator;
import org.apache.drill.exec.physical.config.Limit;
import org.apache.drill.exec.physical.config.MergeJoinPOP;
import org.apache.drill.exec.physical.config.MergingReceiverPOP;
import org.apache.drill.exec.physical.config.OrderedPartitionSender;
import org.apache.drill.exec.physical.config.ProducerConsumer;
import org.apache.drill.exec.physical.config.Project;
import org.apache.drill.exec.physical.config.RangeSender;
import org.apache.drill.exec.physical.config.Screen;
import org.apache.drill.exec.physical.config.SingleSender;
import org.apache.drill.exec.physical.config.Sort;
import org.apache.drill.exec.physical.config.StreamingAggregate;
import org.apache.drill.exec.physical.config.Trace;
import org.apache.drill.exec.physical.config.UnionAll;
import org.apache.drill.exec.physical.config.UnionExchange;
import org.apache.drill.exec.physical.config.UnorderedReceiver;

public abstract class AbstractPhysicalVisitor<T, X, E extends Throwable> implements PhysicalVisitor<T, X, E> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AbstractPhysicalVisitor.class);

  @Override
  public T visitExchange(Exchange exchange, X value) throws E{
    return visitOp(exchange, value);
  }

  @Override
  public T visitUnion(UnionAll union, X value) throws E {
    return visitOp(union, value);
  }

  @Override
  public T visitWriter(Writer writer, X value) throws E {
    return visitOp(writer, value);
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
  public T visitTrace(Trace trace, X value) throws E{
      return visitOp(trace, value);
  }

  @Override
  public T visitSort(Sort sort, X value) throws E{
    return visitOp(sort, value);
  }

  @Override
  public T visitLimit(Limit limit, X value) throws E {
    return visitOp(limit, value);
  }

  @Override
  public T visitStreamingAggregate(StreamingAggregate agg, X value) throws E {
    return visitOp(agg, value);
  }

  @Override
  public T visitHashAggregate(HashAggregate agg, X value) throws E {
    return visitOp(agg, value);
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
  public T visitGroupScan(GroupScan groupScan, X value) throws E{
    return visitOp(groupScan, value);
  }

  @Override
  public T visitSubScan(SubScan subScan, X value) throws E{
    return visitOp(subScan, value);
  }

  @Override
  public T visitStore(Store store, X value) throws E{
    return visitOp(store, value);
  }


  public T visitChildren(PhysicalOperator op, X value) throws E{
    for (PhysicalOperator child : op) {
      child.accept(this, value);
    }
    return null;
  }

  @Override
  public T visitMergeJoin(MergeJoinPOP join, X value) throws E {
    return visitOp(join, value);
  }

  @Override
  public T visitHashJoin(HashJoinPOP join, X value) throws E {
    return visitOp(join, value);
  }

  @Override
  public T visitHashPartitionSender(HashPartitionSender op, X value) throws E {
    return visitSender(op, value);
  }

  @Override
  public T visitOrderedPartitionSender(OrderedPartitionSender op, X value) throws E {
    return visitSender(op, value);
  }

  @Override
  public T visitUnorderedReceiver(UnorderedReceiver op, X value) throws E {
    return visitReceiver(op, value);
  }

  @Override
  public T visitMergingReceiver(MergingReceiverPOP op, X value) throws E {
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
  public T visitBroadcastSender(BroadcastSender op, X value) throws E {
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
  public T visitProducerConsumer(ProducerConsumer op, X value) throws E {
    return visitOp(op, value);
  }

  @Override
  public T visitIteratorValidator(IteratorValidator op, X value) throws E {
    return visitOp(op, value);
  }

  @Override
  public T visitOp(PhysicalOperator op, X value) throws E{
    throw new UnsupportedOperationException(String.format(
        "The PhysicalVisitor of type %s does not currently support visiting the PhysicalOperator type %s.", this
            .getClass().getCanonicalName(), op.getClass().getCanonicalName()));
  }

}
