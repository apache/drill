package org.apache.drill.exec.store.maprdb.json;

import org.apache.drill.exec.store.hbase.HBaseUtils;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.Bytes;
import org.ojai.store.QueryCondition;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.mapr.db.MapRDB;
import com.mapr.db.impl.ConditionImpl;

public class JsonScanSpec {
	protected String tableName;
	protected QueryCondition condition;
	
	@JsonCreator
	public JsonScanSpec(@JsonProperty("tableName") String tableName,
	                    @JsonProperty("condition") QueryCondition condition) {
	  this.tableName = tableName;
	  this.condition = condition;
  }

  public String getTableName() {
    return this.tableName;
  }

  public byte[] getStartRow() {
    if (condition == null) {
      return HConstants.EMPTY_START_ROW;
    }
    return ((ConditionImpl)this.condition).getRowkeyRanges().get(0).getStartRow();
  }

  public byte[] getStopRow() {
    if (condition == null) {
      return HConstants.EMPTY_END_ROW;
    }
    
    return ((ConditionImpl)this.condition).getRowkeyRanges().get(0).getStopRow();
  }

  public Object getSerializedFilter() {
    if (this.condition != null) {
      return ((ConditionImpl)this.condition).getDescriptor().getSerialized();
    }

    return null;
  }

  public void setCondition(QueryCondition condition) {
    this.condition = condition;
  }

  @JsonIgnore
  public QueryCondition getCondition() {
    return this.condition;
  }

  public void mergeScanSpec(String functionName, JsonScanSpec scanSpec) {

    if (this.condition != null && scanSpec.getCondition() != null) {
      QueryCondition newCond = MapRDB.newCondition();
      switch (functionName) {
      case "booleanAnd":
        newCond.and();
        break;
      case "booleanOr":
        newCond.or();
        break;
        default:
          assert(false);
      }

      newCond.condition(this.condition)
             .condition(scanSpec.getCondition())
             .close()
             .build();

      this.condition = newCond;
    } else if (scanSpec.getCondition() != null){
      this.condition = scanSpec.getCondition();
    }
  }
  
  @Override
  public String toString() {
    return "JsonScanSpec [tableName=" + tableName
        + ", condition=" + (condition == null ? null : condition.toString())
        + "]";
  }

}
