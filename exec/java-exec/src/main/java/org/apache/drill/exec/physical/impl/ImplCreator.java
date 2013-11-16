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

import java.util.Collections;
import java.util.List;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.base.AbstractPhysicalVisitor;
import org.apache.drill.exec.physical.base.FragmentRoot;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.SubScan;
import org.apache.drill.exec.physical.config.*;
import org.apache.drill.exec.physical.impl.aggregate.AggBatchCreator;
import org.apache.drill.exec.physical.impl.filter.FilterBatchCreator;
import org.apache.drill.exec.physical.impl.join.MergeJoinCreator;
import org.apache.drill.exec.physical.impl.limit.LimitBatchCreator;
import org.apache.drill.exec.physical.impl.orderedpartitioner.OrderedPartitionBatchCreator;
import org.apache.drill.exec.physical.impl.partitionsender.PartitionSenderCreator;
import org.apache.drill.exec.physical.impl.project.ProjectBatchCreator;
import org.apache.drill.exec.physical.impl.sort.SortBatchCreator;
import org.apache.drill.exec.physical.impl.svremover.SVRemoverCreator;
import org.apache.drill.exec.physical.impl.trace.TraceBatchCreator;
import org.apache.drill.exec.physical.impl.union.UnionBatchCreator;
import org.apache.drill.exec.physical.impl.validate.IteratorValidatorCreator;
import org.apache.drill.exec.physical.impl.validate.IteratorValidatorInjector;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.store.json.JSONScanBatchCreator;
import org.apache.drill.exec.store.json.JSONSubScan;
import org.apache.drill.exec.store.mock.MockScanBatchCreator;
import org.apache.drill.exec.store.mock.MockSubScanPOP;
import org.apache.drill.exec.store.parquet.ParquetRowGroupScan;
import org.apache.drill.exec.store.parquet.ParquetScanBatchCreator;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Implementation of the physical operator visitor
 */
public class ImplCreator extends AbstractPhysicalVisitor<RecordBatch, FragmentContext, ExecutionSetupException>{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ImplCreator.class);

  // Element creators supported by this visitor
  private MockScanBatchCreator msc = new MockScanBatchCreator();
  private ParquetScanBatchCreator parquetScan = new ParquetScanBatchCreator();
  private ScreenCreator sc = new ScreenCreator();
  private MergingReceiverCreator mrc = new MergingReceiverCreator();
  private RandomReceiverCreator rrc = new RandomReceiverCreator();
  private PartitionSenderCreator hsc = new PartitionSenderCreator();
  private OrderedPartitionBatchCreator opc = new OrderedPartitionBatchCreator();
  private SingleSenderCreator ssc = new SingleSenderCreator();
  private ProjectBatchCreator pbc = new ProjectBatchCreator();
  private OrderedPartitionBatchCreator smplbc = new OrderedPartitionBatchCreator();
  private FilterBatchCreator fbc = new FilterBatchCreator();
  private LimitBatchCreator lbc = new LimitBatchCreator();
  private UnionBatchCreator unionbc = new UnionBatchCreator();
  private SVRemoverCreator svc = new SVRemoverCreator();
  private SortBatchCreator sbc = new SortBatchCreator();
  private AggBatchCreator abc = new AggBatchCreator();
  private MergeJoinCreator mjc = new MergeJoinCreator();
  private IteratorValidatorCreator ivc = new IteratorValidatorCreator();
  private RootExec root = null;
  private TraceBatchCreator tbc = new TraceBatchCreator();

  private ImplCreator(){}

  public RootExec getRoot(){
    return root;
  }

  @Override
  public RecordBatch visitProject(Project op, FragmentContext context) throws ExecutionSetupException {
    return pbc.getBatch(context, op, getChildren(op, context));
  }

  @Override
  public RecordBatch visitTrace(Trace op, FragmentContext context) throws ExecutionSetupException {
    return tbc.getBatch(context, op, getChildren(op, context));
  }
  @Override
  public RecordBatch visitSubScan(SubScan subScan, FragmentContext context) throws ExecutionSetupException {
    Preconditions.checkNotNull(subScan);
    Preconditions.checkNotNull(context);

    if (subScan instanceof MockSubScanPOP) {
      return msc.getBatch(context, (MockSubScanPOP) subScan, Collections.<RecordBatch> emptyList());
    } else if (subScan instanceof JSONSubScan) {
      return new JSONScanBatchCreator().getBatch(context, (JSONSubScan) subScan, Collections.<RecordBatch> emptyList());
    } else if (subScan instanceof ParquetRowGroupScan) {
      return parquetScan.getBatch(context, (ParquetRowGroupScan) subScan, Collections.<RecordBatch> emptyList());
    } else {
      return super.visitSubScan(subScan, context);
    }

  }

  @Override
  public RecordBatch visitOp(PhysicalOperator op, FragmentContext context) throws ExecutionSetupException {
    if (op instanceof SelectionVectorRemover) {
      return svc.getBatch(context, (SelectionVectorRemover) op, getChildren(op, context));
    } else {
      return super.visitOp(op, context);
    }
  }

  @Override
  public RecordBatch visitSort(Sort sort, FragmentContext context) throws ExecutionSetupException {
    return sbc.getBatch(context, sort, getChildren(sort, context));
  }

  @Override
  public RecordBatch visitLimit(Limit limit, FragmentContext context) throws ExecutionSetupException {
    return lbc.getBatch(context, limit, getChildren(limit, context));
  }

  @Override
  public RecordBatch visitMergeJoin(MergeJoinPOP op, FragmentContext context) throws ExecutionSetupException {
    return mjc.getBatch(context, op, getChildren(op, context));
  }

  @Override
  public RecordBatch visitScreen(Screen op, FragmentContext context) throws ExecutionSetupException {
    Preconditions.checkArgument(root == null);
    root = sc.getRoot(context, op, getChildren(op, context));
    return null;
  }

  @Override
  public RecordBatch visitHashPartitionSender(HashPartitionSender op, FragmentContext context) throws ExecutionSetupException {
    root = hsc.getRoot(context, op, getChildren(op, context));
    return null;
  }

  @Override
  public RecordBatch visitOrderedPartitionSender(OrderedPartitionSender op, FragmentContext context) throws ExecutionSetupException {
    List<RecordBatch> children = Lists.newArrayList();
    children.add(opc.getBatch(context, op, getChildren(op, context)));
    HashPartitionSender config = new HashPartitionSender(op.getOppositeMajorFragmentId(), op, op.getRef(),op.getDestinations());
    root = hsc.getRoot(context, config, children);
    return null;
  }
  
  @Override
  public RecordBatch visitFilter(Filter filter, FragmentContext context) throws ExecutionSetupException {
    return fbc.getBatch(context, filter, getChildren(filter, context));
  }


  @Override
  public RecordBatch visitStreamingAggregate(StreamingAggregate config, FragmentContext context)
      throws ExecutionSetupException {
    return abc.getBatch(context, config, getChildren(config, context));
  }

  @Override
  public RecordBatch visitUnion(Union union, FragmentContext context) throws ExecutionSetupException {
    return unionbc.getBatch(context, union, getChildren(union, context));
  }

  @Override
  public RecordBatch visitSingleSender(SingleSender op, FragmentContext context) throws ExecutionSetupException {
    root = ssc.getRoot(context, op, getChildren(op, context));
    return null;
  }

  @Override
  public RecordBatch visitRandomReceiver(RandomReceiver op, FragmentContext context) throws ExecutionSetupException {
    return rrc.getBatch(context, op, null);
  }

  @Override
  public RecordBatch visitMergingReceiver(MergingReceiverPOP op, FragmentContext context) throws ExecutionSetupException {
    return mrc.getBatch(context, op, null);
  }

  private List<RecordBatch> getChildren(PhysicalOperator op, FragmentContext context) throws ExecutionSetupException {
    List<RecordBatch> children = Lists.newArrayList();
    for (PhysicalOperator child : op) {
      children.add(child.accept(this, context));
    }
    return children;
  }

  @Override
  public RecordBatch visitIteratorValidator(IteratorValidator op, FragmentContext context) throws ExecutionSetupException {
    return ivc.getBatch(context, op, getChildren(op, context));
  }
  
  public static RootExec getExec(FragmentContext context, FragmentRoot root) throws ExecutionSetupException {
    ImplCreator i = new ImplCreator();
    boolean isAssertEnabled = false;
    assert isAssertEnabled = true;
    if(isAssertEnabled){
      root = IteratorValidatorInjector.rewritePlanWithIteratorValidator(context, root);
    }
    root.accept(i, context);
    if (i.root == null)
      throw new ExecutionSetupException(
          "The provided fragment did not have a root node that correctly created a RootExec value.");
    return i.getRoot();
  }


}
