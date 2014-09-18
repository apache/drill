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
package org.apache.drill.exec.physical.impl.join;

import java.util.List;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.MergeJoinPOP;
import org.apache.drill.exec.physical.impl.BatchCreator;
import org.apache.drill.exec.record.RecordBatch;
import org.eigenbase.rel.JoinRelType;

import com.google.common.base.Preconditions;

public class MergeJoinCreator implements BatchCreator<MergeJoinPOP> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MergeJoinCreator.class);

  @Override
  public RecordBatch getBatch(FragmentContext context, MergeJoinPOP config, List<RecordBatch> children) throws ExecutionSetupException {
    Preconditions.checkArgument(children.size() == 2);
    if(config.getJoinType() == JoinRelType.RIGHT){
      return new MergeJoinBatch(config.flipIfRight(), context, children.get(1), children.get(0));
    }else{
      return new MergeJoinBatch(config, context, children.get(0), children.get(1));
    }

  }
}
