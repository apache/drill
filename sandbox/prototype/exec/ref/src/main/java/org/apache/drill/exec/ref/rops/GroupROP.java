package org.apache.drill.exec.ref.rops;

import org.apache.drill.common.logical.data.Group;
import org.apache.drill.common.logical.data.NamedExpression;
import org.apache.drill.exec.ref.RecordIterator;
import org.apache.drill.exec.ref.RecordPointer;
import org.apache.drill.exec.ref.eval.EvaluatorFactory;
import org.apache.drill.exec.ref.eval.EvaluatorTypes.BasicEvaluator;
import org.apache.drill.exec.ref.rops.MultiLevelMap.GroupingEntry;
import org.apache.drill.exec.ref.rops.MultiLevelMap.MultiLevelIterator;

public class GroupROP extends AbstractBlockingOperator<Group> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(GroupROP.class);
  
  MultiLevelMap map;
  
  public GroupROP(Group g){
    super(g);
  }

  @Override
  protected void setupEvals(EvaluatorFactory builder) {
    NamedExpression[] groupings = config.getGroupings();
    BasicEvaluator[] evals = new BasicEvaluator[groupings.length];
    for(int i =0; i < groupings.length; i++){
      evals[i] = builder.getBasicEvaluator(inputRecord, groupings[i].getExpr());
    }
    map = new MultiLevelMap(evals);
  }


  @Override
  protected void consumeRecord() {
    map.add(inputRecord.copy());
  }

  
  @Override
  protected RecordIterator doWork() {
    return new LevelIterator(map.getIterator());
  }


  private class LevelIterator implements RecordIterator {

    final MultiLevelIterator iter;

    public LevelIterator(MultiLevelIterator iter) {
      super();
      this.iter = iter;
    }

    @Override
    public NextOutcome next() {
      GroupingEntry e = iter.next();
      if(e == null){
        return NextOutcome.NONE_LEFT;
      }
      outputRecord.setRecord(e.record);
      return NextOutcome.INCREMENTED_SCHEMA_CHANGED;
    }

    @Override
    public ROP getParent() {
      return GroupROP.this;
    }

    @Override
    public RecordPointer getRecordPointer() {
      return outputRecord;
    }
  }

  
}
