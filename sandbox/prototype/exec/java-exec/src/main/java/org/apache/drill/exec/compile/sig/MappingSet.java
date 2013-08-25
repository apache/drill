package org.apache.drill.exec.compile.sig;

import java.util.Arrays;

import org.apache.drill.exec.expr.DirectExpression;

import com.google.common.base.Preconditions;
import com.sun.codemodel.JExpr;
import com.sun.codemodel.JExpression;


public class MappingSet {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MappingSet.class);
  
  private GeneratorMapping constant;
  private GeneratorMapping[] mappings;
  private int mappingIndex;
  private GeneratorMapping current;
  private DirectExpression readIndex;
  private DirectExpression writeIndex;
  private DirectExpression incoming;
  private DirectExpression outgoing;
  
  
  public MappingSet(GeneratorMapping mapping) {
    this("inIndex", "outIndex", new GeneratorMapping[]{mapping, mapping});
  }
  
  public MappingSet(GeneratorMapping... mappings) {
    this("inIndex", "outIndex", mappings);
  }

  public MappingSet(String readIndex, String writeIndex, GeneratorMapping... mappings) {
    this(readIndex, writeIndex, "incoming", "outgoing", mappings);
  }
  
  public MappingSet(String readIndex, String writeIndex, String incoming, String outgoing, GeneratorMapping... mappings) {
    this.readIndex = DirectExpression.direct(readIndex);
    this.writeIndex = DirectExpression.direct(writeIndex);
    this.incoming = DirectExpression.direct(incoming);
    this.outgoing = DirectExpression.direct(outgoing);
    Preconditions.checkArgument(mappings.length >= 2);
    this.constant = mappings[0];
    this.mappings = Arrays.copyOfRange(mappings, 1, mappings.length);
    this.current = this.mappings[0];
  }

  public void enterConstant(){
//    assert constant != current;
//    current = constant;
  }
  
  public void exitConstant(){
//    assert constant == current;
//    current = mappings[mappingIndex];
  }
  
  
  public void enterChild(){
    assert current == mappings[mappingIndex];
    mappingIndex++;
    if(mappingIndex >= mappings.length) throw new IllegalStateException("This generator does not support mappings beyond");
    current = mappings[mappingIndex];
  }
  
  public void exitChild(){
    assert current == mappings[mappingIndex];
    mappingIndex--;
    if(mappingIndex < 0) throw new IllegalStateException("You tried to traverse higher than the provided mapping provides.");
    current = mappings[mappingIndex];
  }
  
  public GeneratorMapping getCurrentMapping(){
    return current;
  }
  
  public DirectExpression getValueWriteIndex(){
    return writeIndex;
  }
  
  public DirectExpression getValueReadIndex(){
    return readIndex;
  }
  
  public DirectExpression getOutgoing(){
    return outgoing;
  }
  
  public DirectExpression getIncoming(){
    return incoming;
  }
}
