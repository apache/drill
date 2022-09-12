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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.shaded.guava.com.google.common.graph.ImmutableValueGraph;
import org.apache.drill.shaded.guava.com.google.common.graph.ValueGraphBuilder;

public class ResolverTypePrecedence {

  // Cost of casting between primitive types
  public static final float PRIMITIVE_TYPE_COST = 1f;
  // Base cost of casting
  public static final float BASE_COST = 10f;

  // A weighted directed graph that represents the cost of casting between
  // pairs of data types. The edge weights represent casting preferences and
  // it is important to note that only some of these prefereces can be
  // understood in terms of factors like computational cost or loss of
  // precision. The others are derived from the expected behaviour of the query
  // engine in the face of various data types and queries as expressed by the
  // test suite.
  public static final ImmutableValueGraph<MinorType, Float> CAST_GRAPH = ValueGraphBuilder
    .directed()
    .<MinorType, Float>immutable()

    // null type source vertex (null is castable to any type)
    // NULL casting preference: BIT > INT > FLOAT4 > DECIMAL9 > VARCHAR > ... > DICT
    .putEdgeValue(MinorType.NULL, MinorType.BIT, PRIMITIVE_TYPE_COST)
    .putEdgeValue(MinorType.NULL, MinorType.INT, 2*PRIMITIVE_TYPE_COST)
    .putEdgeValue(MinorType.NULL, MinorType.FLOAT4, 3*PRIMITIVE_TYPE_COST)
    .putEdgeValue(MinorType.NULL, MinorType.DECIMAL9, BASE_COST)
    .putEdgeValue(MinorType.NULL, MinorType.VARCHAR, 2*BASE_COST)
    .putEdgeValue(MinorType.NULL, MinorType.DATE, 3*BASE_COST)
    .putEdgeValue(MinorType.NULL, MinorType.INTERVALDAY, 3*BASE_COST)
    .putEdgeValue(MinorType.NULL, MinorType.MONEY, 3*BASE_COST)
    .putEdgeValue(MinorType.NULL, MinorType.DICT, 4*BASE_COST)

    // bit conversions
    // prefer to cast VARCHAR to BIT than BIT to numerics
    .putEdgeValue(MinorType.BIT, MinorType.TINYINT, 10*BASE_COST)
    .putEdgeValue(MinorType.BIT, MinorType.UINT1, 10*BASE_COST)

    // unsigned int widening
    .putEdgeValue(MinorType.UINT1, MinorType.UINT2, PRIMITIVE_TYPE_COST)
    .putEdgeValue(MinorType.UINT2, MinorType.UINT4, PRIMITIVE_TYPE_COST)
    .putEdgeValue(MinorType.UINT4, MinorType.UINT8, PRIMITIVE_TYPE_COST)
    .putEdgeValue(MinorType.UINT8, MinorType.VARDECIMAL, BASE_COST)
    // unsigned int conversions
    // prefer to cast UINTs to BIGINT over FLOAT4
    .putEdgeValue(MinorType.UINT4, MinorType.BIGINT, PRIMITIVE_TYPE_COST)
    .putEdgeValue(MinorType.UINT4, MinorType.FLOAT4, 2*PRIMITIVE_TYPE_COST)
    .putEdgeValue(MinorType.UINT8, MinorType.FLOAT4, 2*PRIMITIVE_TYPE_COST)

    // int widening
    .putEdgeValue(MinorType.TINYINT, MinorType.SMALLINT, PRIMITIVE_TYPE_COST)
    .putEdgeValue(MinorType.SMALLINT, MinorType.INT, PRIMITIVE_TYPE_COST)
    .putEdgeValue(MinorType.INT, MinorType.BIGINT, PRIMITIVE_TYPE_COST)
    // int conversions
    // prefer to cast INTs to BIGINT over FLOAT4
    .putEdgeValue(MinorType.INT, MinorType.FLOAT4, 2*PRIMITIVE_TYPE_COST)
    .putEdgeValue(MinorType.BIGINT, MinorType.FLOAT4, 2*PRIMITIVE_TYPE_COST)
    .putEdgeValue(MinorType.BIGINT, MinorType.VARDECIMAL, BASE_COST)

    // float widening
    .putEdgeValue(MinorType.FLOAT4, MinorType.FLOAT8, PRIMITIVE_TYPE_COST)
    // float conversions
    .putEdgeValue(MinorType.FLOAT8, MinorType.VARDECIMAL, BASE_COST)

    // decimal widening
    .putEdgeValue(MinorType.DECIMAL9, MinorType.DECIMAL18, BASE_COST)
    .putEdgeValue(MinorType.DECIMAL18, MinorType.DECIMAL28SPARSE, BASE_COST)
    .putEdgeValue(MinorType.DECIMAL28SPARSE, MinorType.DECIMAL28DENSE, BASE_COST)
    .putEdgeValue(MinorType.DECIMAL28DENSE, MinorType.DECIMAL38SPARSE, BASE_COST)
    .putEdgeValue(MinorType.DECIMAL38SPARSE, MinorType.DECIMAL38DENSE, BASE_COST)
    .putEdgeValue(MinorType.DECIMAL38DENSE, MinorType.VARDECIMAL, BASE_COST)
    .putEdgeValue(MinorType.MONEY, MinorType.VARDECIMAL, BASE_COST)
    // decimal conversions
    // VARDECIMAL casting preference: FLOAT8 > INT > VARCHAR
    .putEdgeValue(MinorType.VARDECIMAL, MinorType.FLOAT8, BASE_COST)
    .putEdgeValue(MinorType.VARDECIMAL, MinorType.INT, 2*BASE_COST)
    // prefer the cast in the opposite direction
    .putEdgeValue(MinorType.VARDECIMAL, MinorType.VARCHAR, 10*BASE_COST)

    // interval widening
    .putEdgeValue(MinorType.INTERVALDAY, MinorType.INTERVALYEAR, BASE_COST)
    .putEdgeValue(MinorType.INTERVALYEAR, MinorType.INTERVAL, BASE_COST)
    // interval conversions
    // prefer the cast in the opposite direction
    .putEdgeValue(MinorType.INTERVAL, MinorType.VARCHAR, 10*BASE_COST)

    // dict widening
    .putEdgeValue(MinorType.DICT, MinorType.MAP, BASE_COST)

    // timestamp widening
    .putEdgeValue(MinorType.DATE, MinorType.TIMESTAMP, BASE_COST)
    .putEdgeValue(MinorType.TIMESTAMP, MinorType.TIMESTAMPTZ, BASE_COST)
    .putEdgeValue(MinorType.TIME, MinorType.TIMETZ, BASE_COST)
    // timestamp conversions
    // TIMESTAMP casting preference: DATE > TIME > VARCHAR
    // prefer the casts in the opposite directions
    .putEdgeValue(MinorType.TIMESTAMP, MinorType.DATE, 2*BASE_COST)
    .putEdgeValue(MinorType.TIMESTAMP, MinorType.TIME, 3*BASE_COST)
    .putEdgeValue(MinorType.TIMESTAMPTZ, MinorType.VARCHAR, 10*BASE_COST)
    .putEdgeValue(MinorType.TIMETZ, MinorType.VARCHAR, 10*BASE_COST)

    // char and binary widening
    .putEdgeValue(MinorType.FIXEDBINARY, MinorType.VARBINARY, BASE_COST)
    .putEdgeValue(MinorType.FIXEDCHAR, MinorType.VARCHAR, BASE_COST)
    // char and binary conversions
    // VARCHAR casting preference: INT > FLOAT8 > VARDECIMAL > TIMESTAMP > INTERVALDAY > BIT > VARBINARY
    .putEdgeValue(MinorType.VARCHAR, MinorType.INT, BASE_COST)
    .putEdgeValue(MinorType.VARCHAR, MinorType.FLOAT8, 2*BASE_COST)
    .putEdgeValue(MinorType.VARCHAR, MinorType.VARDECIMAL, 3*BASE_COST)
    .putEdgeValue(MinorType.VARCHAR, MinorType.TIMESTAMP, 4*BASE_COST)
    .putEdgeValue(MinorType.VARCHAR, MinorType.INTERVALDAY, 5*BASE_COST)
    .putEdgeValue(MinorType.VARCHAR, MinorType.BIT, 6*BASE_COST)
    .putEdgeValue(MinorType.VARCHAR, MinorType.VARBINARY, 7*BASE_COST)

    // union type sink vertex
    .putEdgeValue(MinorType.LIST, MinorType.UNION, BASE_COST)
    .putEdgeValue(MinorType.MAP, MinorType.UNION, BASE_COST)
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
        // Goal found. We only wanted to calculate the path distance so we
        // don't need to go on to backtrace it.
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
