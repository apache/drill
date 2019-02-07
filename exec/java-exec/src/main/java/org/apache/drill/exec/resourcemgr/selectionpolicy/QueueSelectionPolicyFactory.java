/*
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
package org.apache.drill.exec.resourcemgr.selectionpolicy;

public class QueueSelectionPolicyFactory {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(QueueSelectionPolicyFactory.class);

  public static QueueSelectionPolicy createSelectionPolicy(String policy) {
    QueueSelectionPolicy selectionPolicy;
    if (policy.equals("default")) {
      selectionPolicy = new DefaultQueueSelection();
    } else if (policy.equals("bestfit")) {
      selectionPolicy = new BestFitQueueSelection();
    } else if (policy.equals("random")) {
      selectionPolicy = new RandomQueueSelection();
    } else {
      logger.info("QueueSelectionPolicy is not configured so proceeding with the bestfit for now");
      selectionPolicy = new BestFitQueueSelection();
    }

    return selectionPolicy;
  }
}
