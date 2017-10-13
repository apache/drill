/*
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
 */
package org.apache.drill.common.expression;

import java.io.IOException;
import java.util.Iterator;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;
import org.apache.drill.common.expression.PathSegment.ArraySegment;
import org.apache.drill.common.expression.PathSegment.NameSegment;
import org.apache.drill.common.expression.parser.ExprLexer;
import org.apache.drill.common.expression.parser.ExprParser;
import org.apache.drill.common.expression.parser.ExprParser.parse_return;
import org.apache.drill.common.expression.visitors.ExprVisitor;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.proto.UserBitShared.NamePart;
import org.apache.drill.exec.proto.UserBitShared.NamePart.Type;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;

public class SchemaPath extends LogicalExpressionBase {

  private final NameSegment rootSegment;

  public static SchemaPath getSimplePath(String name) {
    return getCompoundPath(name);
  }

  public static SchemaPath getCompoundPath(String... strings) {
    NameSegment s = null;
    // loop through strings in reverse order
    for (int i = strings.length - 1; i >= 0; i--) {
      s = new NameSegment(strings[i], s);
    }
    return new SchemaPath(s);
  }

  public PathSegment getLastSegment() {
    PathSegment s= rootSegment;
    while (s.getChild() != null) {
      s = s.getChild();
    }
    return s;
  }

  @Deprecated
  public SchemaPath(String simpleName, ExpressionPosition pos) {
    super(pos);
    this.rootSegment = new NameSegment(simpleName);
  }


  public NamePart getAsNamePart() {
    return getNamePart(rootSegment);
  }

  private static NamePart getNamePart(PathSegment s) {
    if (s == null) {
      return null;
    }
    NamePart.Builder b = NamePart.newBuilder();
    if (s.getChild() != null) {
      b.setChild(getNamePart(s.getChild()));
    }

    if (s.isArray()) {
      if (s.getArraySegment().hasIndex()) {
        throw new IllegalStateException("You cannot convert a indexed schema path to a NamePart.  NameParts can only reference Vectors, not individual records or values.");
      }
      b.setType(Type.ARRAY);
    } else {
      b.setType(Type.NAME);
      b.setName(s.getNameSegment().getPath());
    }
    return b.build();
  }

  private static PathSegment getPathSegment(NamePart n) {
    PathSegment child = n.hasChild() ? getPathSegment(n.getChild()) : null;
    if (n.getType() == Type.ARRAY) {
      return new ArraySegment(child);
    } else {
      return new NameSegment(n.getName(), child);
    }
  }

  public static SchemaPath create(NamePart namePart) {
    Preconditions.checkArgument(namePart.getType() == NamePart.Type.NAME);
    return new SchemaPath((NameSegment) getPathSegment(namePart));
  }

  /**
   * Parses input string using the same rules which are used for the field in the query.
   * If a string contains dot outside back-ticks, or there are no backticks in the string,
   * will be created {@link SchemaPath} with the {@link NameSegment}
   * which contains one else {@link NameSegment}, etc.
   * If a string contains [] then {@link ArraySegment} will be created.
   *
   * @param expr input string to be parsed
   * @return {@link SchemaPath} instance
   */
  public static SchemaPath parseFromString(String expr) {
    if (expr == null || expr.isEmpty()) {
      return null;
    }
    try {
      ExprLexer lexer = new ExprLexer(new ANTLRStringStream(expr));
      CommonTokenStream tokens = new CommonTokenStream(lexer);
      ExprParser parser = new ExprParser(tokens);

      parse_return ret = parser.parse();

      if (ret.e instanceof SchemaPath) {
        return (SchemaPath) ret.e;
      } else {
        throw new IllegalStateException("Schema path is not a valid format.");
      }
    } catch (RecognitionException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * A simple is a path where there are no repeated elements outside the lowest level of the path.
   * @return Whether this path is a simple path.
   */
  public boolean isSimplePath() {
    PathSegment seg = rootSegment;
    while (seg != null) {
      if (seg.isArray() && !seg.isLastPath()) {
        return false;
      }
      seg = seg.getChild();
    }
    return true;
  }


  public SchemaPath(SchemaPath path) {
    super(path.getPosition());
    this.rootSegment = path.rootSegment;
  }

  public SchemaPath(NameSegment rootSegment) {
    super(ExpressionPosition.UNKNOWN);
    this.rootSegment = rootSegment;
  }

  public SchemaPath(NameSegment rootSegment, ExpressionPosition pos) {
    super(pos);
    this.rootSegment = rootSegment;
  }

  @Override
  public <T, V, E extends Exception> T accept(ExprVisitor<T, V, E> visitor, V value) throws E {
    return visitor.visitSchemaPath(this, value);
  }

  public SchemaPath getChild(String childPath) {
    NameSegment newRoot = rootSegment.cloneWithNewChild(new NameSegment(childPath));
    return new SchemaPath(newRoot);
  }

  public SchemaPath getUnindexedArrayChild() {
    NameSegment newRoot = rootSegment.cloneWithNewChild(new ArraySegment(null));
    return new SchemaPath(newRoot);
  }

  public SchemaPath getChild(int index) {
    NameSegment newRoot = rootSegment.cloneWithNewChild(new ArraySegment(index));
    return new SchemaPath(newRoot);
  }

  public NameSegment getRootSegment() {
    return rootSegment;
  }

  @Override
  public MajorType getMajorType() {
    return Types.LATE_BIND_TYPE;
  }

  @Override
  public int hashCode() {
    return ((rootSegment == null) ? 0 : rootSegment.hashCode());
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof SchemaPath)) {
      return false;
    }

    SchemaPath other = (SchemaPath) obj;
    if (rootSegment == null) {
      return (other.rootSegment == null);
    }
    return rootSegment.equals(other.rootSegment);
  }

  public boolean contains(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof SchemaPath)) {
      return false;
    }

    SchemaPath other = (SchemaPath) obj;
    return rootSegment == null || rootSegment.contains(other.rootSegment);
  }

  @Override
  public Iterator<LogicalExpression> iterator() {
    return Iterators.emptyIterator();
  }

  @Override
  public String toString() {
    return ExpressionStringBuilder.toString(this);
  }

  public String toExpr() {
    return ExpressionStringBuilder.toString(this);
  }

  /**
   * Returns path string of {@code rootSegment}
   *
   * @return path string of {@code rootSegment}
   */
  public String getRootSegmentPath() {
    return rootSegment.getPath();
  }

  public static class De extends StdDeserializer<SchemaPath> {

    public De() {
      super(LogicalExpression.class);
    }

    @Override
    public SchemaPath deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException {
      return parseFromString(jp.getText());
    }
  }
}
