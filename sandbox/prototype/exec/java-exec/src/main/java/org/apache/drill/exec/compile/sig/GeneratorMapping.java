package org.apache.drill.exec.compile.sig;

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

  public static GeneratorMapping GM(String setup, String eval, String reset, String cleanup){
    return new GeneratorMapping(setup, eval, reset, cleanup);
  }

  public String getSetup() {
    return setup;
  }

  public String getEval() {
    return eval;
  }

  public String getReset() {
    return reset;
  }

  public String getCleanup() {
    return cleanup;
  }
 
  
}
