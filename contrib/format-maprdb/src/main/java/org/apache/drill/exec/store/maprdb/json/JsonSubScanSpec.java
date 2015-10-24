package org.apache.drill.exec.store.maprdb.json;

import java.nio.ByteBuffer;

import org.apache.drill.exec.store.maprdb.MapRDBSubScanSpec;
import org.apache.hadoop.hbase.HConstants;
import org.bouncycastle.util.Arrays;
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
        Arrays.areEqual(startRow, HConstants.EMPTY_START_ROW) == false) {
      Value startVal = IdCodec.decode(startRow);
      
      switch(startVal.getType()) {
      case BINARY:
        this.condition.is(DocumentConstants.ID_FIELD, Op.GREATER_OR_EQUAL, startVal.getBinary());
        break;
      case STRING:
        this.condition.is(DocumentConstants.ID_FIELD, Op.LESS, startVal.getString());
        break;
      default:
        throw new IllegalStateException("Encountered an unsupported type " + startVal.getType()
                                        + " for _id");
      }
    }
    
    if (stopRow != null &&
        Arrays.areEqual(stopRow, HConstants.EMPTY_END_ROW) == false) {
      Value stopVal = IdCodec.decode(stopRow);
      
      switch(stopVal.getType()) {
      case BINARY:
        this.condition.is(DocumentConstants.ID_FIELD, Op.GREATER_OR_EQUAL, stopVal.getBinary());
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
