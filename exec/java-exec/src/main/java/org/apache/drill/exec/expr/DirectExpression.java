package org.apache.drill.exec.expr;

import com.sun.codemodel.JExpressionImpl;
import com.sun.codemodel.JFormatter;

public class DirectExpression extends JExpressionImpl{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DirectExpression.class);
  
  final String source;
  
  private DirectExpression(final String source) {
    super();
    this.source = source;
  }
  
  public void generate( JFormatter f ) {
    f.p('(').p(source).p(')');
  }
  
  public static DirectExpression direct( final String source ) {
    return new DirectExpression(source);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((source == null) ? 0 : source.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    DirectExpression other = (DirectExpression) obj;
    if (source == null) {
      if (other.source != null)
        return false;
    } else if (!source.equals(other.source))
      return false;
    return true;
  }
  
  
}
