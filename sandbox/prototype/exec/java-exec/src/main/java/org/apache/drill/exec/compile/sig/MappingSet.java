package org.apache.drill.exec.compile.sig;

import java.util.Arrays;

import com.google.common.base.Preconditions;
import com.sun.codemodel.JExpr;
import com.sun.codemodel.JExpression;


public class MappingSet {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MappingSet.class);
  
  private GeneratorMapping constant;
  private GeneratorMapping[] mappings;
  private int mappingIndex;
  private GeneratorMapping current;
  private JExpression readIndex;
  private JExpression writeIndex;
  
  
  public MappingSet(String readIndex, String writeIndex, GeneratorMapping... mappings) {
    this.readIndex = JExpr.direct(readIndex);
    this.writeIndex = JExpr.direct(writeIndex);
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
  }
  
  public GeneratorMapping getCurrentMapping(){
    return current;
  }
  
  public JExpression getValueWriteIndex(){
    return writeIndex;
  }
  
  public JExpression getValueReadIndex(){
    return readIndex;
  }
  
  
}
