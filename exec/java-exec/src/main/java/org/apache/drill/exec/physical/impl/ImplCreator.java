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

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.drill.common.AutoCloseables;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.base.FragmentRoot;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.impl.validate.IteratorValidatorInjector;
import org.apache.drill.exec.record.CloseableRecordBatch;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.util.AssertionUtil;
import org.apache.drill.exec.util.ImpersonationUtil;
import org.apache.hadoop.security.UserGroupInformation;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;

/**
 * Create RecordBatch tree (PhysicalOperator implementations) for a given PhysicalOperator tree.
 */
public class ImplCreator {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ImplCreator.class);

  private final LinkedList<CloseableRecordBatch> operators = Lists.newLinkedList();

  private ImplCreator() {}

  private List<CloseableRecordBatch> getOperators() {
    return operators;
  }

  /**
   * Create and return fragment RootExec for given FragmentRoot. RootExec has one or more RecordBatches as children
   * (which may contain child RecordBatches and so on).
   *
   * @param context
   *          FragmentContext.
   * @param root
   *          FragmentRoot.
   * @return RootExec of fragment.
   * @throws ExecutionSetupException
   */
  public static RootExec getExec(FragmentContext context, FragmentRoot root) throws ExecutionSetupException {
    Preconditions.checkNotNull(root);
    Preconditions.checkNotNull(context);

    if (AssertionUtil.isAssertionsEnabled()) {
      root = IteratorValidatorInjector.rewritePlanWithIteratorValidator(context, root);
    }
    final ImplCreator creator = new ImplCreator();
    Stopwatch watch = new Stopwatch();
    watch.start();

    try {
      final RootExec rootExec = creator.getRootExec(root, context);
      // skip over this for SimpleRootExec (testing)
      if (rootExec instanceof BaseRootExec) {
        ((BaseRootExec) rootExec).setOperators(creator.getOperators());
      }

      logger.debug("Took {} ms to create RecordBatch tree", watch.elapsed(TimeUnit.MILLISECONDS));
      if (rootExec == null) {
        throw new ExecutionSetupException(
            "The provided fragment did not have a root node that correctly created a RootExec value.");
      }

      return rootExec;
    } catch(Exception e) {
      e.printStackTrace();
      context.fail(e);
      for(final CloseableRecordBatch crb : creator.getOperators()) {
        AutoCloseables.close(crb, logger);
      }
    }

    return null;
  }

  /** Create RootExec and its children (RecordBatches) for given FragmentRoot */
  private RootExec getRootExec(final FragmentRoot root, final FragmentContext context) throws ExecutionSetupException {
    final List<RecordBatch> childRecordBatches = getChildren(root, context);

    if (context.isImpersonationEnabled()) {
      final UserGroupInformation proxyUgi = ImpersonationUtil.createProxyUgi(root.getUserName(), context.getQueryUserName());
      try {
        return proxyUgi.doAs(new PrivilegedExceptionAction<RootExec>() {
          @Override
          public RootExec run() throws Exception {
            return ((RootCreator<PhysicalOperator>) getOpCreator(root, context)).getRoot(context, root, childRecordBatches);
          }
        });
      } catch (InterruptedException | IOException e) {
        final String errMsg = String.format("Failed to create RootExec for operator with id '%d'", root.getOperatorId());
        logger.error(errMsg, e);
        throw new ExecutionSetupException(errMsg, e);
      }
    } else {
      return ((RootCreator<PhysicalOperator>) getOpCreator(root, context)).getRoot(context, root, childRecordBatches);
    }
  }


  /** Create a RecordBatch and its children for given PhysicalOperator */
  private RecordBatch getRecordBatch(final PhysicalOperator op, final FragmentContext context) throws ExecutionSetupException {
    Preconditions.checkNotNull(op);

    final List<RecordBatch> childRecordBatches = getChildren(op, context);

    if (context.isImpersonationEnabled()) {
      final UserGroupInformation proxyUgi = ImpersonationUtil.createProxyUgi(op.getUserName(), context.getQueryUserName());
      try {
        return proxyUgi.doAs(new PrivilegedExceptionAction<RecordBatch>() {
          @Override
          public RecordBatch run() throws Exception {
            final CloseableRecordBatch batch = ((BatchCreator<PhysicalOperator>) getOpCreator(op, context)).getBatch(
                context, op, childRecordBatches);
            operators.addFirst(batch);
            return batch;
          }
        });
      } catch (InterruptedException | IOException e) {
        final String errMsg = String.format("Failed to create RecordBatch for operator with id '%d'", op.getOperatorId());
        logger.error(errMsg, e);
        throw new ExecutionSetupException(errMsg, e);
      }
    } else {
      final CloseableRecordBatch batch = ((BatchCreator<PhysicalOperator>) getOpCreator(op, context)).getBatch(context,
          op, childRecordBatches);
      operators.addFirst(batch);
      return batch;
    }
  }

  /** Helper method to get OperatorCreator (RootCreator or BatchCreator) for given PhysicalOperator (root or non-root) */
  private Object getOpCreator(PhysicalOperator op, final FragmentContext context) throws ExecutionSetupException {
    final Class opClass = op.getClass();
    Object opCreator = context.getDrillbitContext().getOperatorCreatorRegistry().getOperatorCreator(opClass);
    if (opCreator == null) {
      throw new UnsupportedOperationException(
          String.format("BatchCreator for PhysicalOperator type '%s' not found.", opClass.getCanonicalName()));
    }

    return opCreator;
  }

  /** Helper method to traverse the children of given PhysicalOperator and create RecordBatches for children recursively */
  private List<RecordBatch> getChildren(final PhysicalOperator op, final FragmentContext context) throws ExecutionSetupException {
    List<RecordBatch> children = Lists.newArrayList();
    for (PhysicalOperator child : op) {
      children.add(getRecordBatch(child, context));
    }

    return children;
  }

}
