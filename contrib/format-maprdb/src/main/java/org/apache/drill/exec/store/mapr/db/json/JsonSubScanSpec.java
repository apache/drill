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
package org.apache.drill.exec.store.mapr.db.json;

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.drill.exec.store.mapr.db.MapRDBSubScanSpec;
import org.apache.hadoop.hbase.HConstants;
import org.ojai.DocumentConstants;
import org.ojai.Value;
import org.ojai.store.QueryCondition;
import org.ojai.store.QueryCondition.Op;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.mapr.db.MapRDB;
import com.mapr.db.impl.ConditionImpl;
import com.mapr.db.impl.IdCodec;

public class JsonSubScanSpec extends MapRDBSubScanSpec {

  protected QueryCondition condition;

  @JsonCreator
  public JsonSubScanSpec(@JsonProperty("tableName") String tableName,
                         @JsonProperty("regionServer") String regionServer,
                         @JsonProperty("startRow") byte[] startRow,
                         @JsonProperty("stopRow") byte[] stopRow,
                         @JsonProperty("cond") QueryCondition cond) {
    super(tableName, regionServer, null, null, null, null);

    this.condition = MapRDB.newCondition().and();

    if (cond != null) {
      this.condition.condition(cond);
    }

    if (startRow != null &&
        Arrays.equals(startRow, HConstants.EMPTY_START_ROW) == false) {
      Value startVal = IdCodec.decode(startRow);

      switch(startVal.getType()) {
      case BINARY:
        this.condition.is(DocumentConstants.ID_FIELD, Op.GREATER_OR_EQUAL, startVal.getBinary());
        break;
      case STRING:
        this.condition.is(DocumentConstants.ID_FIELD, Op.GREATER_OR_EQUAL, startVal.getString());
        break;
      default:
        throw new IllegalStateException("Encountered an unsupported type " + startVal.getType()
                                        + " for _id");
      }
    }

    if (stopRow != null &&
        Arrays.equals(stopRow, HConstants.EMPTY_END_ROW) == false) {
      Value stopVal = IdCodec.decode(stopRow);

      switch(stopVal.getType()) {
      case BINARY:
        this.condition.is(DocumentConstants.ID_FIELD, Op.LESS, stopVal.getBinary());
        break;
      case STRING:
        this.condition.is(DocumentConstants.ID_FIELD, Op.LESS, stopVal.getString());
        break;
      default:
        throw new IllegalStateException("Encountered an unsupported type " + stopVal.getType()
                                        + " for _id");
      }
    }

    this.condition.close().build();
  }

  public void setCondition(QueryCondition cond) {
    condition = cond;
  }

  @JsonIgnore
  public QueryCondition getCondition() {
    return this.condition;
  }

  @Override
  public byte[] getSerializedFilter() {
    if (this.condition != null) {
      ByteBuffer bbuf = ((ConditionImpl)this.condition).getDescriptor().getSerialized();
      byte[] serFilter = new byte[bbuf.limit() - bbuf.position()];
      bbuf.get(serFilter);
      return serFilter;
    }

    return null;
  }
}
