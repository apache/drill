/*******************************************************************************
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package org.apache.drill.common.expression;

import org.apache.drill.common.expression.ValueExpressions.CollisionBehavior;



public abstract class PathSegment{

  private final ValueExpressions.CollisionBehavior collision;
  private PathSegment child;

  protected PathSegment(CollisionBehavior collision){
    this.collision = collision;
  }
  
  public static class ArraySegment extends PathSegment{
    private final int index;
    
    public ArraySegment(int index, ValueExpressions.CollisionBehavior collision){
      super(collision);
      if(index < 0 ) throw new IllegalArgumentException();
      this.index = index;
    }
    
    public int getIndex(){
      return index;
    }
    
    public boolean isArray(){
      return true;
    }
    
    public boolean isNamed(){
      return false;
    }

    @Override
    public ArraySegment getArraySegment() {
      return this;
    }

    @Override
    public String toString() {
      return "ArraySegment [index=" + index + ", getCollisionBehavior()=" + getCollisionBehavior() + ", getChild()="
          + getChild() + "]";
    }
    
    
    
  }
  
  
  
  public static class NameSegment extends PathSegment{
    private final CharSequence path;
    
    public NameSegment(CharSequence n, ValueExpressions.CollisionBehavior collision){
      super(collision);
      this.path = n;
    }
    
    public CharSequence getPath(){
      return path;        
    }
    
    public boolean isArray(){
      return false;
    }
    
    public boolean isNamed(){
      return true;
    }

    @Override
    public NameSegment getNameSegment() {
      return this;
    }

    @Override
    public String toString() {
      return "NameSegment [path=" + path + ", getCollisionBehavior()=" + getCollisionBehavior() + ", getChild()="
          + getChild() + "]";
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((path == null) ? 0 : path.hashCode());
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
      NameSegment other = (NameSegment) obj;
      if (path == null) {
        if (other.path != null)
          return false;
      } else if (!path.equals(other.path))
        return false;
      return true;
    }
    
    
    
  }
  
  public NameSegment getNameSegment(){
    throw new UnsupportedOperationException();
  }
  public ArraySegment getArraySegment(){
    throw new UnsupportedOperationException();
  }
  public abstract boolean isArray();
  public abstract boolean isNamed();
  
  
  public ValueExpressions.CollisionBehavior getCollisionBehavior(){
    return collision;
  }
  
  public boolean isLastPath(){
    return child == null;
  }

  public PathSegment getChild() {
    return child;
  }

  public void setChild(PathSegment child) {
    this.child = child;
  }

  
  
}