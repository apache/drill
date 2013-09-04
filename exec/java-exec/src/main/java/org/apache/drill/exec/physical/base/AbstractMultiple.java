package org.apache.drill.exec.physical.base;

import com.google.common.collect.Iterators;

import java.util.Iterator;

/**
 * Describes an operator that expects more than one children operators as its input.
 */
public abstract class AbstractMultiple extends AbstractBase{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AbstractMultiple.class);  
  
  protected final PhysicalOperator[] children;

  protected AbstractMultiple(PhysicalOperator[] children) {
    this.children = children;
  }

  public PhysicalOperator[] getChildren() {
    return children;
  }
  
  @Override
  public Iterator<PhysicalOperator> iterator() {
    return Iterators.forArray(children);
  }
  
  public Size getSize() {
    Size size = new Size(0,0);
    for(PhysicalOperator child:children){
      size.add(child.getSize());
    }
    return size;
  }
    
}
