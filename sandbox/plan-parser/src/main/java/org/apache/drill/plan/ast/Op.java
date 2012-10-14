package org.apache.drill.plan.ast;

import java.util.List;

/**
* Created with IntelliJ IDEA. User: tdunning Date: 10/12/12 Time: 11:23 PM To change this template
* use File | Settings | File Templates.
*/
public class Op {
  private String op;

  private List<Arg> inputs;
  private List<Arg> outputs;


  public static Op create(String op, List<Arg> inputs, List<Arg> outputs) {
    Op r = new Op();
    r.op = op;
    r.inputs = inputs;
    r.outputs = outputs;
    return r;
  }

  public List<Arg> getInputs() {
    return inputs;
  }

  public String getOp() {
    return op;
  }

  public List<Arg> getOutputs() {
    return outputs;
  }
}
