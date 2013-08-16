package org.apache.drill.exec.compile.sig;

import org.apache.drill.exec.expr.CodeGenerator.BlockType;

import com.google.common.base.Preconditions;

public class GeneratorMapping {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(GeneratorMapping.class);

  private String setup;
  private String eval;
  private String reset;
  private String cleanup;
  

  public GeneratorMapping(String setup, String eval, String reset, String cleanup) {
    super();
    this.setup = setup;
    this.eval = eval;
    this.reset = reset;
    this.cleanup = cleanup;
  }

  public static GeneratorMapping GM(String setup, String eval){
    return create(setup, eval, null, null);
  }
  
  public static GeneratorMapping GM(String setup, String eval, String reset, String cleanup){
    return create(setup, eval, reset, cleanup);
  }

  public static GeneratorMapping create(String setup, String eval, String reset, String cleanup){   
    return new GeneratorMapping(setup, eval, reset, cleanup);
  }
  
  public String getMethodName(BlockType type){
    switch(type){
    case CLEANUP:
      Preconditions.checkNotNull(cleanup, "The current mapping does not have a cleanup method defined.");
      return cleanup;
    case EVAL:
      Preconditions.checkNotNull(eval, "The current mapping does not have an eval method defined.");
      return eval;
    case RESET:
      Preconditions.checkNotNull(reset, "The current mapping does not have a cleanup method defined.");
      return reset;
    case SETUP:
      Preconditions.checkNotNull(setup, "The current mapping does not have a setup method defined.");
      return setup;
    default:
      throw new IllegalStateException();
    }
  }
  
  
}
