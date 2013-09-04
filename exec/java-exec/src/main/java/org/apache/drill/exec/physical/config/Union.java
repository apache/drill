package org.apache.drill.exec.physical.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.drill.exec.physical.OperatorCost;
import org.apache.drill.exec.physical.base.*;

import java.util.List;

@JsonTypeName("union")

public class Union extends AbstractMultiple {

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Filter.class);

  @JsonCreator
  public Union(@JsonProperty("children") PhysicalOperator[] children) {
    super(children);
  }

  @Override
  public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E {
    return physicalVisitor.visitUnion(this, value);
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) {
    return new Union(children.toArray(new PhysicalOperator[children.size()]));
  }

  @Override
  public OperatorCost getCost() {
    OperatorCost cost = new OperatorCost(0,0,0,0);
    for (int i = 0; i < children.length; i++) {
      PhysicalOperator child = children[i];
      cost.add(child.getCost());
    }
    return cost;
  }

}
