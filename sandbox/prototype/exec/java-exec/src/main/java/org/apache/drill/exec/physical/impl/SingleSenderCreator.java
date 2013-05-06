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
package org.apache.drill.exec.physical.impl;

import java.util.List;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.SingleSender;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.record.FragmentWritableBatch;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.RecordBatch.IterOutcome;
import org.apache.drill.exec.rpc.bit.BitTunnel;

public class SingleSenderCreator implements RootCreator<SingleSender>{

  @Override
  public RootExec getRoot(FragmentContext context, SingleSender config, List<RecordBatch> children)
      throws ExecutionSetupException {
    assert children != null && children.size() == 1;
    return new SingleSenderRootExec(context, children.iterator().next(), config);
  }
  
  
  private static class SingleSenderRootExec implements RootExec{
    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SingleSenderRootExec.class);
    private RecordBatch incoming;
    private BitTunnel tunnel;
    private FragmentHandle handle;
    private int recMajor;
    private FragmentContext context;
    
    public SingleSenderRootExec(FragmentContext context, RecordBatch batch, SingleSender config){
      logger.debug("Creating single sender root exec base on config: {}", config);
      this.incoming = batch;
      this.handle = context.getHandle();
      this.recMajor = config.getOppositeMajorFragmentId();
      this.tunnel = context.getCommunicator().getTunnel(config.getDestination());
      this.context = context;
    }
    
    @Override
    public boolean next() {
      IterOutcome out = incoming.next();
      logger.debug("Outcome of sender next {}", out);
      switch(out){
      case STOP:
      case NONE:
        FragmentWritableBatch b2 = new FragmentWritableBatch(false, handle.getQueryId(), handle.getMajorFragmentId(), handle.getMinorFragmentId(), recMajor, 0, incoming.getWritableBatch());
        tunnel.sendRecordBatch(context, b2);
        return false;
        

      case OK:
      case OK_NEW_SCHEMA:
        FragmentWritableBatch batch = new FragmentWritableBatch(true, handle.getQueryId(), handle.getMajorFragmentId(), handle.getMinorFragmentId(), recMajor, 0, incoming.getWritableBatch());
        tunnel.sendRecordBatch(context, batch);
        return true;

      case NOT_YET:
      default:
        throw new IllegalStateException();
      }
    }

    @Override
    public void stop() {
    }
    
    
    
  }
}
