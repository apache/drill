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
package org.apache.drill.exec.work.batch;


import static org.junit.Assert.assertEquals;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.ExecTest;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.proto.BitData.FragmentRecordBatch;
import org.apache.drill.exec.record.RawFragmentBatch;
import org.apache.drill.exec.rpc.Response;
import org.apache.drill.exec.rpc.ResponseSender;
import org.apache.drill.exec.rpc.data.DataRpcConfig;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 * Test case to test whether backpressure is applied when
 * size of the queue of RawBatchBuffers is exceeding specified softLimit.
 * It is testing that acknowledgments are queued and sent according to the
 * correct schedule
 * If algorithm to release acks will be changed in the future
 * this test will need to be changed
 * It is not testing whether Senders receive acknowledgments and act accordingly
 */
public class TestUnlimitedBatchBuffer extends ExecTest {

  private static int FRAGMENT_COUNT = 5;
  private DrillConfig dc = DrillConfig.create();

  private static class MySender implements ResponseSender {

    private int sendCount = 0;

    @Override
    public void send(Response r) {
      sendCount++;
    }


    public int getSendCount() {
      return sendCount;
    }

    public void resetSender() {
      sendCount = 0;
    }
  }
  @Test
  public void testBackPressure() throws Exception {

    final MySender mySender = new MySender();
    FragmentContext context = Mockito.mock(FragmentContext.class);

    Mockito.when(context.getConfig()).thenReturn(dc);

    UnlimitedRawBatchBuffer rawBuffer = new UnlimitedRawBatchBuffer(context, FRAGMENT_COUNT);

    RawFragmentBatch batch = Mockito.mock(RawFragmentBatch.class);

    Mockito.when(batch.getSender()).thenReturn(mySender);
    Mockito.doAnswer(new Answer<Void>() {
      public Void answer(InvocationOnMock ignore) throws Throwable {
        mySender.send(DataRpcConfig.OK);
        return null;
      }
    }).when(batch).sendOk();

    FragmentRecordBatch header = FragmentRecordBatch.newBuilder().setIsOutOfMemory(false).setIsLastBatch(false).build();
    Mockito.when(batch.getHeader()).thenReturn(header);

    /// start the real test
    int incomingBufferSize = dc.getInt(ExecConstants.INCOMING_BUFFER_SIZE);
    int softLimit = incomingBufferSize * FRAGMENT_COUNT;

    // No back pressure should be kicked in
    for ( int i = 0; i < softLimit-1; i++) {
      rawBuffer.enqueue(batch);
    }

    // number of responses sent == number of enqueued elements
    assertEquals(softLimit - 1, mySender.getSendCount());
    rawBuffer.getNext();

    // set senderCount to 0
    mySender.resetSender();

    // test back pressure
    // number of elements in the queue = softLimit -2
    // enqueue softlimit elements more
    for ( int i = 0; i < softLimit; i++) {
      rawBuffer.enqueue(batch);
    }
    // we are exceeding softlimit, so senderCount should not increase
    assertEquals(1, mySender.getSendCount());

    // other responses should be saved in the responsequeue
    for (int i = 0; i < softLimit-2; i++ ) {
      rawBuffer.getNext();
    }

    // still should not send responses, as queue.size should higher then softLimit
    assertEquals(1, mySender.getSendCount());

    // size of the queue == softLimit now
    for (int i = softLimit; i > 0 ; i-- ) {
      int senderCount = mySender.getSendCount();
      rawBuffer.getNext();
      int expectedCountNumber = softLimit - i + senderCount+1;
      assertEquals((expectedCountNumber < softLimit ? expectedCountNumber : softLimit), mySender.getSendCount());
    }
  }

}
