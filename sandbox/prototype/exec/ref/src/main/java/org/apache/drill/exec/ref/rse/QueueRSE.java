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
package org.apache.drill.exec.ref.rse;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.logical.StorageEngineConfigBase;
import org.apache.drill.common.logical.data.Store;
import org.apache.drill.exec.ref.RecordPointer;
import org.apache.drill.exec.ref.RunOutcome.OutcomeType;
import org.apache.drill.exec.ref.exceptions.SetupException;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

public class QueueRSE extends RSEBase {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(QueueRSE.class);

  private DrillConfig dConfig;
  private final List<Queue<Object>> sinkQueues;
  
  public QueueRSE(QueueRSEConfig engineConfig, DrillConfig dConfig) throws SetupException{
    this.dConfig = dConfig;
    sinkQueues = Collections.singletonList( (Queue<Object>) (new ArrayBlockingQueue<Object>(100)));
  }

  public Queue<Object> getQueue(int number){
    return sinkQueues.get(number);
  }
  
  @JsonTypeName("queue")
  public static class QueueRSEConfig extends StorageEngineConfigBase {
    @JsonCreator
    public QueueRSEConfig(@JsonProperty("name") String name) {
      super(name);
    }
  }
  
  public static class QueueOutputInfo{
    public int number;
  }

  public boolean supportsWrite() {
    return true;
  }

  
  @Override
  public RecordRecorder getWriter(Store store) throws IOException {
    QueueOutputInfo config = store.getTarget().getWith(QueueOutputInfo.class);
    Queue<Object> q = dConfig.getQueue(config.number);
    return new QueueRecordRecorder(q);
  }

  
  private class QueueRecordRecorder implements RecordRecorder{

    private final Queue<Object> queue;
    
    public QueueRecordRecorder(Queue<Object> queue) {
      this.queue = queue;
    }

    @Override
    public void setup() throws IOException {
    }

    @Override
    public long recordRecord(RecordPointer r) throws IOException {
      final ByteArrayOutputStream baos = new ByteArrayOutputStream();
      final JSONDataWriter writer = new JSONDataWriter(baos);
      r.write(writer);
      writer.finish();
      queue.add(baos.toByteArray());
      return 0;
    }

    @Override
    public void finish(OutcomeType type) throws IOException {
      queue.add(type);
    }
    
  }
  
  
}
