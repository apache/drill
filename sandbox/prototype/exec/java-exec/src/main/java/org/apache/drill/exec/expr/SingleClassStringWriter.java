package org.apache.drill.exec.expr;

import java.io.IOException;
import java.io.OutputStream;
import java.io.StringWriter;
import java.io.Writer;

import com.google.common.base.Preconditions;
import com.sun.codemodel.CodeWriter;
import com.sun.codemodel.JPackage;

public class SingleClassStringWriter extends CodeWriter{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SingleClassStringWriter.class);

  private boolean used;
  private StringWriter writer = new StringWriter();
  
  @Override
  public OutputStream openBinary(JPackage pkg, String fileName) throws IOException {
    throw new UnsupportedOperationException();
  }

  
  @Override
  public Writer openSource(JPackage pkg, String fileName) throws IOException {
    Preconditions.checkArgument(!used, "The SingleClassStringWriter can only output once src file.");
    used = true;
    return writer;
  }

  @Override
  public void close() throws IOException {
  }

  public StringBuffer getCode(){
    return writer.getBuffer();
  }
  
  
}
