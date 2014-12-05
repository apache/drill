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
package org.apache.drill.exec.rpc.data;

import io.netty.buffer.DrillBuf;

import org.apache.drill.exec.exception.FragmentSetupException;
import org.apache.drill.exec.proto.BitData.FragmentRecordBatch;
import org.apache.drill.exec.proto.BitData.RpcType;
import org.apache.drill.exec.record.RawFragmentBatch;
import org.apache.drill.exec.rpc.Acks;
import org.apache.drill.exec.rpc.RemoteConnection;
import org.apache.drill.exec.rpc.Response;
import org.apache.drill.exec.rpc.ResponseSender;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.work.WorkManager.WorkerBee;
import org.apache.drill.exec.work.fragment.FragmentManager;

import java.io.IOException;

public class DataResponseHandlerImpl implements DataResponseHandler{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DataResponseHandlerImpl.class);

  private final WorkerBee bee;

  public DataResponseHandlerImpl(WorkerBee bee) {
    super();
    this.bee = bee;
  }


  @Override
  public void informOutOfMemory() {
    logger.error("Out of memory outside any particular fragment.");
  }


  public void handle(RemoteConnection connection, FragmentManager manager, FragmentRecordBatch fragmentBatch, DrillBuf data, ResponseSender sender) throws RpcException {
//    logger.debug("Fragment Batch received {}", fragmentBatch);
    try {
      boolean canRun = manager.handle(new RawFragmentBatch(connection, fragmentBatch, data, sender));
      if (canRun) {
//        logger.debug("Arriving batch means local batch can run, starting local batch.");
        // if we've reached the canRun threshold, we'll proceed. This expects handler.handle() to only return a single
        // true.
        bee.startFragmentPendingRemote(manager);
      }
      if (fragmentBatch.getIsLastBatch() && !manager.isWaiting()) {
//        logger.debug("Removing handler.  Is Last Batch {}.  Is Waiting for more {}", fragmentBatch.getIsLastBatch(),
//            manager.isWaiting());
        bee.getContext().getWorkBus().removeFragmentManager(manager.getHandle());
      }

    } catch (FragmentSetupException e) {
      logger.error("Failure while attempting to setup new fragment.", e);
      sender.send(new Response(RpcType.ACK, Acks.FAIL));
    } catch (IOException e) {
      throw new RpcException(e);
    }
  }
}
