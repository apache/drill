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
package org.apache.drill.exec.resolver;

import java.util.*;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableSet;
import org.apache.drill.shaded.guava.com.google.common.graph.ImmutableValueGraph;
import org.apache.drill.shaded.guava.com.google.common.graph.ValueGraphBuilder;

public class ResolverTypePrecedence {

  private static final Set<MinorType> UNSUPPORTED_TYPES = ImmutableSet.of(
    MinorType.TINYINT,
    MinorType.SMALLINT,
    MinorType.UINT1,
    MinorType.UINT2
  );

  // Cost of casting between primitive types
  public static final float PRIMITIVE_TYPE_COST = 1f;
  // Base cost of casting
  public static final float BASE_COST = 10f;
  // Cost of casting with the possibility of a loss of precision.
  // Chosen to definitely be larger than any path of BASE_COST
  // edges across the implicit casting graph.
  public static final float PRECISION_LOSS_COST = 1e4f;

  // A weighted directed graph that represents the cost of casting between
  // pairs of data types. Coefficients (e.g. 2*BASE_COST) are used in cases
  // where we want to indicate that casting to some type is preferred over
  // some other choice e.g. it is preferred to cast a UINT4 to a BIGINT rather
  // than to a FLOAT8 though both are possible without a loss of precision.
  public static final ImmutableValueGraph<MinorType, Float> CAST_GRAPH = ValueGraphBuilder
    .directed()
    .<MinorType, Float>immutable()

    // null type source vertex (null is castable to any type)
    .putEdgeValue(MinorType.NULL, MinorType.BIT, PRIMITIVE_TYPE_COST)
    .putEdgeValue(MinorType.NULL, MinorType.FLOAT4, PRIMITIVE_TYPE_COST)
    .putEdgeValue(MinorType.NULL, MinorType.DECIMAL9, BASE_COST)
    .putEdgeValue(MinorType.NULL, MinorType.INTERVALDAY, BASE_COST)
    .putEdgeValue(MinorType.NULL, MinorType.MAP, BASE_COST)
    .putEdgeValue(MinorType.NULL, MinorType.DATE, BASE_COST)

    // unsigned int widening
    .putEdgeValue(MinorType.BIT, MinorType.UINT1, PRIMITIVE_TYPE_COST)
    .putEdgeValue(MinorType.UINT1, MinorType.UINT2, PRIMITIVE_TYPE_COST)
    .putEdgeValue(MinorType.UINT2, MinorType.UINT4, PRIMITIVE_TYPE_COST)
    .putEdgeValue(MinorType.UINT4, MinorType.UINT8, PRIMITIVE_TYPE_COST)
    .putEdgeValue(MinorType.UINT8, MinorType.VARDECIMAL, BASE_COST)

    // signed int widening
    .putEdgeValue(MinorType.BIT, MinorType.TINYINT, PRIMITIVE_TYPE_COST)
    .putEdgeValue(MinorType.TINYINT, MinorType.SMALLINT, PRIMITIVE_TYPE_COST)
    .putEdgeValue(MinorType.SMALLINT, MinorType.INT, PRIMITIVE_TYPE_COST)
    .putEdgeValue(MinorType.INT, MinorType.BIGINT, PRIMITIVE_TYPE_COST)
    .putEdgeValue(MinorType.BIGINT, MinorType.VARDECIMAL, BASE_COST)

    // float widening
    .putEdgeValue(MinorType.FLOAT4, MinorType.FLOAT8, PRIMITIVE_TYPE_COST)
    .putEdgeValue(MinorType.FLOAT8, MinorType.VARDECIMAL, BASE_COST)

    // decimal widening
    .putEdgeValue(MinorType.DECIMAL9, MinorType.DECIMAL18, BASE_COST)
    .putEdgeValue(MinorType.DECIMAL18, MinorType.DECIMAL28SPARSE, BASE_COST)
    .putEdgeValue(MinorType.DECIMAL28SPARSE, MinorType.DECIMAL28DENSE, BASE_COST)
    .putEdgeValue(MinorType.DECIMAL28DENSE, MinorType.DECIMAL38SPARSE, BASE_COST)
    .putEdgeValue(MinorType.DECIMAL38SPARSE, MinorType.DECIMAL38DENSE, BASE_COST)
    .putEdgeValue(MinorType.DECIMAL38DENSE, MinorType.VARDECIMAL, BASE_COST)
    .putEdgeValue(MinorType.MONEY, MinorType.VARDECIMAL, BASE_COST)

    // interval widening
    .putEdgeValue(MinorType.INTERVALDAY, MinorType.INTERVALYEAR, BASE_COST)
    .putEdgeValue(MinorType.INTERVALYEAR, MinorType.INTERVAL, BASE_COST)

    // char and binary widening
    .putEdgeValue(MinorType.FIXEDBINARY, MinorType.VARBINARY, BASE_COST)
    .putEdgeValue(MinorType.FIXEDCHAR, MinorType.VARCHAR, BASE_COST)

    // dict widening
    .putEdgeValue(MinorType.DICT, MinorType.MAP, BASE_COST)

    // date and time widening
    .putEdgeValue(MinorType.DATE, MinorType.TIMESTAMP, BASE_COST)
    .putEdgeValue(MinorType.TIMESTAMP, MinorType.TIMESTAMPTZ, BASE_COST)
    .putEdgeValue(MinorType.TIME, MinorType.TIMETZ, BASE_COST)

    // timestamp component projection
    .putEdgeValue(MinorType.TIMESTAMP, MinorType.DATE, PRECISION_LOSS_COST)
    .putEdgeValue(MinorType.TIMESTAMP, MinorType.TIME, PRECISION_LOSS_COST)

    // unsigned ints to numerics
    .putEdgeValue(MinorType.UINT4, MinorType.BIGINT, PRIMITIVE_TYPE_COST)
    .putEdgeValue(MinorType.UINT4, MinorType.FLOAT8, 2*PRIMITIVE_TYPE_COST)
    .putEdgeValue(MinorType.UINT8, MinorType.FLOAT8, PRECISION_LOSS_COST)
    .putEdgeValue(MinorType.UINT8, MinorType.VARDECIMAL, BASE_COST)

    // ints to numerics
    .putEdgeValue(MinorType.INT, MinorType.FLOAT8, 2*PRIMITIVE_TYPE_COST)
    .putEdgeValue(MinorType.BIGINT, MinorType.FLOAT8, PRECISION_LOSS_COST)
    .putEdgeValue(MinorType.BIGINT, MinorType.VARDECIMAL, BASE_COST)

    // varchars to numerics
    .putEdgeValue(MinorType.VARCHAR, MinorType.VARDECIMAL, PRECISION_LOSS_COST)
    .putEdgeValue(MinorType.VARCHAR, MinorType.FLOAT8, 2* PRECISION_LOSS_COST)
    .putEdgeValue(MinorType.VARCHAR, MinorType.BIGINT, 3* PRECISION_LOSS_COST)

    // varchar -> varbinary -> union type sink vertex
    .putEdgeValue(MinorType.VARDECIMAL, MinorType.VARCHAR, BASE_COST)
    .putEdgeValue(MinorType.LIST, MinorType.VARCHAR, BASE_COST)
    .putEdgeValue(MinorType.TIMESTAMPTZ, MinorType.VARCHAR, BASE_COST)
    .putEdgeValue(MinorType.TIMETZ, MinorType.VARCHAR, BASE_COST)
    .putEdgeValue(MinorType.VARCHAR, MinorType.VARBINARY, BASE_COST)
    .putEdgeValue(MinorType.VARBINARY, MinorType.UNION, BASE_COST)

    .build();

  /**
   * Searches the implicit casting graph for the path of least total cost using
   * Dijkstra's algorithm.
   * @param fromType type to cast from
   * @param toType type to cast to
   * @return a positive float path cost or +∞ if no path exists
   */
  public static float computeCost(MinorType fromType, MinorType toType) {
    if (UNSUPPORTED_TYPES.contains(fromType) || UNSUPPORTED_TYPES.contains(toType)) {
      return Float.POSITIVE_INFINITY;
    }

    TreeSet<VertexDatum> remaining = new TreeSet<>();
    Map<MinorType, VertexDatum> vertexData = new HashMap<>();
    Set<MinorType> shortestPath = new HashSet<>();

    VertexDatum sourceDatum = new VertexDatum(fromType, 0, null);
    remaining.add(sourceDatum);
    vertexData.put(fromType, sourceDatum);

    while (!remaining.isEmpty()) {
      // Poll the datum with lowest totalDistance in remaining
      VertexDatum vertexDatum = remaining.pollFirst();
      MinorType vertex = vertexDatum.vertex;
      shortestPath.add(vertex);

      if (vertex.equals(toType)) {
        // Goal found. We only wanted to the path distance so don't need to go
        // on to backtrace it.
        return vertexDatum.totalDistance;
      }

      Set<MinorType> successors = CAST_GRAPH.successors(vertex);
      for (MinorType successor : successors) {
        if (shortestPath.contains(successor)) {
          continue;
        }

        float distance = CAST_GRAPH.edgeValue(vertex, successor).orElseThrow(IllegalStateException::new);
        float totalDistance = vertexDatum.totalDistance + distance;

        VertexDatum successorDatum = vertexData.get(successor);
        if (successorDatum == null) {
          successorDatum = new VertexDatum(successor, totalDistance, vertexDatum);
          vertexData.put(successor, successorDatum);
          remaining.add(successorDatum);
        } else if (totalDistance < successorDatum.totalDistance) {
          successorDatum.totalDistance = totalDistance;
          successorDatum.predecessor = vertexDatum;
        }
      }
    }

    return Float.POSITIVE_INFINITY;
  }

  public static final Map<MinorType, Integer> oldPrecedenceMap;

  static {
    /* The oldPrecedenceMap is used to decide whether it's allowed to implicitly "promote"
     * one type to another type.
     *
     * The order that each type is inserted into HASHMAP decides its precedence.
     * First in ==> lowest precedence.
     * A type of lower precedence can be implicitly "promoted" to type of higher precedence.
     * For instance, NULL could be promoted to any other type;
     * tinyint could be promoted into int; but int could NOT be promoted into tinyint (due to possible precision loss).
     */
    int i = 0;
    oldPrecedenceMap = new HashMap<>();
    oldPrecedenceMap.put(MinorType.NULL, i += 2);       // NULL is legal to implicitly be promoted to any other type
    oldPrecedenceMap.put(MinorType.FIXEDBINARY, i += 2); // Fixed-length is promoted to var length
    oldPrecedenceMap.put(MinorType.VARBINARY, i += 2);
    oldPrecedenceMap.put(MinorType.FIXEDCHAR, i += 2);
    oldPrecedenceMap.put(MinorType.VARCHAR, i += 2);
    oldPrecedenceMap.put(MinorType.FIXED16CHAR, i += 2);
    oldPrecedenceMap.put(MinorType.VAR16CHAR, i += 2);
    oldPrecedenceMap.put(MinorType.BIT, i += 2);
    oldPrecedenceMap.put(MinorType.TINYINT, i += 2);   //type with few bytes is promoted to type with more bytes ==> no data loss.
    oldPrecedenceMap.put(MinorType.UINT1, i += 2);     //signed is legal to implicitly be promoted to unsigned.
    oldPrecedenceMap.put(MinorType.SMALLINT, i += 2);
    oldPrecedenceMap.put(MinorType.UINT2, i += 2);
    oldPrecedenceMap.put(MinorType.INT, i += 2);
    oldPrecedenceMap.put(MinorType.UINT4, i += 2);
    oldPrecedenceMap.put(MinorType.BIGINT, i += 2);
    oldPrecedenceMap.put(MinorType.UINT8, i += 2);
    oldPrecedenceMap.put(MinorType.MONEY, i += 2);
    oldPrecedenceMap.put(MinorType.FLOAT4, i += 2);
    oldPrecedenceMap.put(MinorType.DECIMAL9, i += 2);
    oldPrecedenceMap.put(MinorType.DECIMAL18, i += 2);
    oldPrecedenceMap.put(MinorType.DECIMAL28DENSE, i += 2);
    oldPrecedenceMap.put(MinorType.DECIMAL28SPARSE, i += 2);
    oldPrecedenceMap.put(MinorType.DECIMAL38DENSE, i += 2);
    oldPrecedenceMap.put(MinorType.DECIMAL38SPARSE, i += 2);
    oldPrecedenceMap.put(MinorType.VARDECIMAL, i += 2);
    oldPrecedenceMap.put(MinorType.FLOAT8, i += 2);
    oldPrecedenceMap.put(MinorType.DATE, i += 2);
    oldPrecedenceMap.put(MinorType.TIMESTAMP, i += 2);
    oldPrecedenceMap.put(MinorType.TIMETZ, i += 2);
    oldPrecedenceMap.put(MinorType.TIMESTAMPTZ, i += 2);
    oldPrecedenceMap.put(MinorType.TIME, i += 2);
    oldPrecedenceMap.put(MinorType.INTERVALDAY, i+= 2);
    oldPrecedenceMap.put(MinorType.INTERVALYEAR, i+= 2);
    oldPrecedenceMap.put(MinorType.INTERVAL, i+= 2);
    oldPrecedenceMap.put(MinorType.MAP, i += 2);
    oldPrecedenceMap.put(MinorType.DICT, i += 2);
    oldPrecedenceMap.put(MinorType.LIST, i += 2);
    oldPrecedenceMap.put(MinorType.UNION, i += 2);
  }

  /**
   * A struct to hold working data used by Dijkstra's algorithm and allowing
   * comparison based on total distance from the source vertex to this vertex.
   */
  static class VertexDatum implements Comparable<VertexDatum> {
    final MinorType vertex;
    float totalDistance;
    VertexDatum predecessor;

    public VertexDatum(MinorType vertex, float totalDistance, VertexDatum predecessor) {
      this.vertex = vertex;
      this.totalDistance = totalDistance;
      this.predecessor = predecessor;
    }

    @Override
    public int compareTo(VertexDatum other) {
      int distComparison = Float.compare(this.totalDistance, other.totalDistance);
      // TreeSet uses this method to determine member equality, not equals(), so we
      // need to differentiate between vertices with the same totalDistance.
      return distComparison != 0 ? distComparison : this.vertex.compareTo(other.vertex);
    }

    @Override
    public String toString() {
      return String.format("vertex: %s, totalDistance: %f, predecessor: %s", vertex, totalDistance, predecessor);
    }
  }
}
