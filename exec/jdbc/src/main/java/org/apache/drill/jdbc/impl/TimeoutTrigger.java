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
package org.apache.drill.jdbc.impl;

import java.sql.SQLException;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.drill.jdbc.DrillStatement;
import org.apache.drill.jdbc.SqlTimeoutException;

/**
 * Timeout Trigger required for canceling of running queries
 */
class TimeoutTrigger implements Callable<Boolean> {
  private int timeoutInSeconds;
  private AvaticaStatement statementHandle;
  private Future<Boolean> triggerFuture;
  private DrillConnectionImpl connectionHandle;

  public boolean isTriggered() {
    return (triggerFuture != null);
  }


  //Default Constructor is Invalid
  @SuppressWarnings("unused")
  private TimeoutTrigger() {}

  /**
   * Timeout Constructor
   * @param stmtContext   Statement Handle
   * @param timeoutInSec  Timeout defined in seconds
   * @throws SQLException
   */
  TimeoutTrigger(AvaticaStatement stmtContext, int timeoutInSec) throws SQLException {
    timeoutInSeconds = timeoutInSec;
    statementHandle = stmtContext;
    connectionHandle = (DrillConnectionImpl) ((DrillStatement) stmtContext).getConnection();
  }

  @Override
  public Boolean call() throws Exception {
    try {
      TimeUnit.SECONDS.sleep(timeoutInSeconds);
    } catch (InterruptedException e) {
      //Skip interruption that occur due due to query completion
    }
    try {
      if (!statementHandle.isClosed()) {
        // Cancel Statement
        ((DrillStatement) statementHandle).cancelDueToTimeout();
      }
    } catch (SqlTimeoutException toe) {
      throw toe;
    }
    return false;
  }

  /**
   * Start the timer
   * @return
   */
  public Future<Boolean> startCountdown() {
    if (!isTriggered()) {
      return (triggerFuture = connectionHandle.getTimeoutTriggerService().submit(this));
    }
    return triggerFuture;
  }
}
