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
package org.apache.drill.exec.ref;

public class RunOutcome {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RunOutcome.class);
  
  public enum OutcomeType{
    SUCCESS, FAILED, CANCELED;
  }

  public final OutcomeType outcome;
  public final long bytes;
  public final long records;
  public final Throwable exception;
  
  public RunOutcome(OutcomeType outcome, long bytes, long records, Throwable exception) {
    super();
    if(outcome != OutcomeType.SUCCESS) logger.warn("Creating failed outcome.", exception);
    this.outcome = outcome;
    this.bytes = bytes;
    this.records = records;
    this.exception = exception;
  }
  
  public RunOutcome(OutcomeType outcome, long bytes, long records) {
    this(outcome, bytes, records, null);
  }

  @Override
  public String toString() {
    return "RunOutcome [outcome=" + outcome + ", bytes=" + bytes + ", records=" + records + ", exception=" + exception
        + "]";
  }
  

  
}
