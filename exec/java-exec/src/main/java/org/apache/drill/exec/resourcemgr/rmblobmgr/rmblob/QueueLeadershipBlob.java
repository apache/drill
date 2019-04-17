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
package org.apache.drill.exec.resourcemgr.rmblobmgr.rmblob;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

import java.util.Map;
@JsonTypeName(QueueLeadershipBlob.NAME)
public class QueueLeadershipBlob extends AbstractRMStateBlob {
  public static final String NAME = "queue_leaders";

  private Map<String, String> queueLeaders;

  @JsonCreator
  public QueueLeadershipBlob(@JsonProperty("version") int version,
                             @JsonProperty("queueLeaders") Map<String, String> queueLeaders) {
    super(version);
    this.queueLeaders = queueLeaders;
  }

  public Map<String, String> getQueueLeaders() {
    return queueLeaders;
  }

  @Override
  public int hashCode() {
    int result = 31;
    result = result ^ Integer.hashCode(version);
    result = result ^ queueLeaders.hashCode();
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null || (obj.getClass() != this.getClass())) {
      return false;
    }

    if (this == obj) {
      return true;
    }

    QueueLeadershipBlob other = (QueueLeadershipBlob) obj;
    return this.version == other.getVersion() && this.queueLeaders.equals(other.getQueueLeaders());
  }
}