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

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.base.AbstractPhysicalVisitor;
import org.apache.drill.exec.physical.base.FragmentRoot;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.impl.validate.IteratorValidatorInjector;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.util.AssertionUtil;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;

/**
 * Implementation of the physical operator visitor
 */
public class ImplCreator extends AbstractPhysicalVisitor<RecordBatch, FragmentContext, ExecutionSetupException> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ImplCreator.class);

  private RootExec root = null;

  private ImplCreator() {}

  private RootExec getRoot() {
    return root;
  }

  @Override
  @SuppressWarnings("unchecked")
  public RecordBatch visitOp(PhysicalOperator op, FragmentContext context) throws ExecutionSetupException {
    Preconditions.checkNotNull(op);
    Preconditions.checkNotNull(context);

    Object opCreator = context.getDrillbitContext().getOperatorCreatorRegistry().getOperatorCreator(op.getClass());
    if (opCreator != null) {
      if (op instanceof FragmentRoot ) {
        root = ((RootCreator<PhysicalOperator>)opCreator).getRoot(context, op, getChildren(op, context));
        return null;
      } else {
        return ((BatchCreator<PhysicalOperator>)opCreator).getBatch(context, op, getChildren(op, context));
      }
    } else {
      throw new UnsupportedOperationException(String.format(
          "The PhysicalVisitor of type %s does not currently support visiting the PhysicalOperator type %s.",
          this.getClass().getCanonicalName(), op.getClass().getCanonicalName()));
    }
  }

  private List<RecordBatch> getChildren(PhysicalOperator op, FragmentContext context) throws ExecutionSetupException {
    List<RecordBatch> children = Lists.newArrayList();
    for (PhysicalOperator child : op) {
      children.add(child.accept(this, context));
    }
    return children;
  }

  public static RootExec getExec(FragmentContext context, FragmentRoot root) throws ExecutionSetupException {
    ImplCreator i = new ImplCreator();
    if (AssertionUtil.isAssertionsEnabled()) {
      root = IteratorValidatorInjector.rewritePlanWithIteratorValidator(context, root);
    }

    Stopwatch watch = new Stopwatch();
    watch.start();
    root.accept(i, context);
    logger.debug("Took {} ms to accept", watch.elapsed(TimeUnit.MILLISECONDS));
    if (i.root == null) {
      throw new ExecutionSetupException(
          "The provided fragment did not have a root node that correctly created a RootExec value.");
    }
    return i.getRoot();
  }

}
