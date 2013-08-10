package org.apache.drill.exec.compile.sig;


public class CodeGeneratorArgument {
  
  private final String name;
  private final Class<?> type;
  
  public CodeGeneratorArgument(String name, Class<?> type) {
    super();
    this.name = name;
    this.type = type;
  }

  public String getName() {
    return name;
  }

  public Class<?> getType() {
    return type;
  }
  
}
