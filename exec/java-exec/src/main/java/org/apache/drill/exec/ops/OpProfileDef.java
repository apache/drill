package org.apache.drill.exec.ops;

public class OpProfileDef {

  public int operatorId;
  public int operatorType;
  public int incomingCount;

  public int getOperatorId(){
    return operatorId;
  }

  public int getOperatorType(){
    return operatorType;
  }
  public int getIncomingCount(){
    return incomingCount;
  }

}
