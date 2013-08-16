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

import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.drill.common.expression.ValueExpressions.CollisionBehavior;
import org.apache.drill.common.expression.visitors.ExprVisitor;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.Types;

import com.google.common.base.Joiner;
import com.google.common.collect.Iterators;
import com.google.protobuf.DescriptorProtos.UninterpretedOption.NamePart;

public class SchemaPath extends LogicalExpressionBase {

  // reads well in RegexBuddy
  private static final String ENTIRE_REGEX = "^\n" + "(?:                # first match required\n"
      + "\\[\\d+\\]             # array index only\n" + "|\n" + "'?\n" + "[^\\.\\[\\+\\-\\!\\]\\}]+  # identifier\n"
      + "'?\n" + "(?:\\[\\d+\\])?\n" + ")\n" + "[\\+\\-\\!\\]\\}]?\n" +

      "# secondary matches (starts with dot)\n" + "(?:\n" + "\\.\n" + "(?:                # first match required\n"
      + "\\[\\d+\\]             # array index only\n" + "|\n" + "'?\n" + "[^\\.\\[\\+\\-\\!\\]\\}]+  # identifier\n"
      + "'?\n" + "(?:\\[\\d+\\])?\n" + ")\n" + "[\\+\\-\\!\\]\\}]?\n" +

      ")*$";

  // reads well in RegexBuddy
  private static final String SEGMENT_REGEX = "(?:\n" + "\\[(\\d+)\\]\n" + "|\n" + "'?\n"
      + "([^\\.\\[\\+\\-\\!\\]\\}]+)  # identifier\n" + "'?\n" + ")\n"
      + "([\\+\\-\\!\\]\\}]?)         # collision type";
  private static final int GROUP_INDEX = 1;
  private static final int GROUP_PATH_SEGMENT = 2;
  private static final int GROUP_COLLISION = 3;

  private final static Pattern SEGMENT_PATTERN = Pattern.compile(SEGMENT_REGEX, Pattern.COMMENTS);
  private final static Pattern ENTIRE_PATTERN = Pattern.compile(ENTIRE_REGEX, Pattern.COMMENTS);

  private final CharSequence originalPath;
  private final PathSegment rootSegment;

  public SchemaPath(SchemaPath path){
    super(path.getPosition());
    this.originalPath = path.originalPath;
    this.rootSegment = path.rootSegment;
  }
  
  public SchemaPath(CharSequence str, ExpressionPosition pos) {
    super(pos);

    if (!ENTIRE_PATTERN.matcher(str).matches())
      throw new IllegalArgumentException("Identifier doesn't match expected pattern.");
    this.originalPath = str;
    Matcher m = SEGMENT_PATTERN.matcher(str);
    PathSegment r = null;
    PathSegment previous = null;
    PathSegment current;
    while (m.find()) {
      CollisionBehavior col = (m.start(GROUP_COLLISION) != -1) ? CollisionBehavior.find(m.group(GROUP_COLLISION))
          : CollisionBehavior.DEFAULT;

      if (m.start(GROUP_INDEX) != -1) {
        String d = m.group(GROUP_INDEX);
        current = new PathSegment.ArraySegment(Integer.parseInt(d), col);
      } else {
        String i = m.group(GROUP_PATH_SEGMENT);
        current = new PathSegment.NameSegment(i, col);
      }
      if (previous == null) {
        r = current;
      } else {
        previous.setChild(current);
      }
      previous = current;
    }

    rootSegment = r;

  }
  
  private SchemaPath(SchemaPath parent, String[] childPaths){
    super(ExpressionPosition.UNKNOWN);
    SchemaPath p = new SchemaPath(Joiner.on('.').join(childPaths), ExpressionPosition.UNKNOWN);
    this.originalPath = parent.originalPath + "." + p.originalPath;
    PathSegment seg = parent.getRootSegment().clone();
    this.rootSegment = seg;
    while(!seg.isLastPath()) seg = seg.getChild();
    seg.setChild(p.getRootSegment());
  }

  @Override
  public <T, V, E extends Exception> T accept(ExprVisitor<T, V, E> visitor, V value) throws E {
    return visitor.visitSchemaPath(this, value);
  }

  public SchemaPath getChild(String[] childPaths){
    return new SchemaPath(this, childPaths);
  }
  
  public PathSegment getRootSegment() {
    return rootSegment;
  }

  public CharSequence getPath() {
    return originalPath;
  }

  @Override
  public MajorType getMajorType() {
    return Types.LATE_BIND_TYPE;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((rootSegment == null) ? 0 : rootSegment.hashCode());
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
    SchemaPath other = (SchemaPath) obj;
    if (rootSegment == null) {
      if (other.rootSegment != null)
        return false;
    } else if (!rootSegment.equals(other.rootSegment))
      return false;
    return true;
  }

  @Override
  public Iterator<LogicalExpression> iterator() {
    return Iterators.emptyIterator();
  }

  @Override
  public String toString() {
    return "SchemaPath [rootSegment=" + rootSegment + "]";
  }

}