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

  // A weighted directed graph that represents the cost of casting between
  // pairs of data types. The edge weights represent casting preferences and
  // it is important to note that only some of these preferences can be
  // understood in terms of factors like computational cost or loss of
  // precision. The others are derived from the expected behaviour of the query
  // engine in the face of various data types and queries as expressed by the
  // test suite.
  //
  // Bear in mind that this class only establishes which casts will be tried
  // automatically and how they're ranked. See
  // {@link org.apache.drill.exec.resolver.TypeCastRules} for listings of which
  // casts are possible at all.
  public static final ImmutableValueGraph<MinorType, Float> CAST_GRAPH = ValueGraphBuilder
    .directed()
    .<MinorType, Float>immutable()

    // null type source vertex (null is castable to any type)
    // prefer to cast NULL to a non-NULL over any non-NULL to another non-NULL
    .putEdgeValue(MinorType.NULL, MinorType.VARCHAR, 1f)
    .putEdgeValue(MinorType.NULL, MinorType.BIT, 1.1f)
    .putEdgeValue(MinorType.NULL, MinorType.INT, 1.2f)
    .putEdgeValue(MinorType.NULL, MinorType.FLOAT4, 1.3f)
    .putEdgeValue(MinorType.NULL, MinorType.DECIMAL9, 1.4f)
    .putEdgeValue(MinorType.NULL, MinorType.DATE, 1.5f)
    .putEdgeValue(MinorType.NULL, MinorType.INTERVALDAY, 1.6f)
    .putEdgeValue(MinorType.NULL, MinorType.MONEY, 1.7f)
    .putEdgeValue(MinorType.NULL, MinorType.LIST, 1.8f)
    .putEdgeValue(MinorType.NULL, MinorType.DICT, 1.9f)

    // Apart from NULL, casts between differing types have a starting cost
    // of 10f so that when they're summed, a smaller number of casts gets
    // preferred over a larger number.

    // bit conversions
    // prefer to cast VARCHAR to BIT than BIT to numerics
    .putEdgeValue(MinorType.BIT, MinorType.TINYINT, 100f)
    .putEdgeValue(MinorType.BIT, MinorType.UINT1, 100f)

    // unsigned int widening
    .putEdgeValue(MinorType.UINT1, MinorType.UINT2, 10f)
    .putEdgeValue(MinorType.UINT2, MinorType.UINT4, 10f)
    .putEdgeValue(MinorType.UINT4, MinorType.UINT8, 10f)
    .putEdgeValue(MinorType.UINT8, MinorType.VARDECIMAL, 10f)
    // unsigned int conversions
    // prefer to cast UINTs to BIGINT over FLOAT4
    .putEdgeValue(MinorType.UINT4, MinorType.BIGINT, 10f)
    .putEdgeValue(MinorType.UINT4, MinorType.FLOAT4, 11f)
    .putEdgeValue(MinorType.UINT8, MinorType.FLOAT4, 12f)

    // int widening
    .putEdgeValue(MinorType.TINYINT, MinorType.SMALLINT, 10f)
    .putEdgeValue(MinorType.SMALLINT, MinorType.INT, 10f)
    .putEdgeValue(MinorType.INT, MinorType.BIGINT, 10f)
    // int conversions
    .putEdgeValue(MinorType.BIGINT, MinorType.FLOAT4, 10f)
    // prefer to cast INTs to FLOATs over VARDECIMALs
    .putEdgeValue(MinorType.BIGINT, MinorType.VARDECIMAL, 100f)

    // float widening
    .putEdgeValue(MinorType.FLOAT4, MinorType.FLOAT8, 10f)
    // float conversion
    // FLOATs are not currently castable to VARDECIMAL (see TypeCastRules)
    // but it is not possible to avoid some path between them here since
    // FLOATs must ultimately be implicitly castable to VARCHAR, and VARCHAR
    // to VARDECIMAL.
    // prefer the cast in the opposite direction
    .putEdgeValue(MinorType.FLOAT8, MinorType.VARDECIMAL, 10_000f)

    // decimal widening
    .putEdgeValue(MinorType.DECIMAL9, MinorType.DECIMAL18, 10f)
    .putEdgeValue(MinorType.DECIMAL18, MinorType.DECIMAL28DENSE, 10f)
    .putEdgeValue(MinorType.DECIMAL28DENSE, MinorType.DECIMAL28SPARSE, 10f)
    .putEdgeValue(MinorType.DECIMAL28SPARSE, MinorType.DECIMAL38DENSE, 10f)
    .putEdgeValue(MinorType.DECIMAL38DENSE, MinorType.DECIMAL38SPARSE, 10f)
    .putEdgeValue(MinorType.DECIMAL38SPARSE, MinorType.VARDECIMAL, 10f)
    .putEdgeValue(MinorType.MONEY, MinorType.VARDECIMAL, 10f)
    // decimal conversions
    // prefer to cast INTs to VARDECIMALs over VARDECIMALs to FLOATs
    .putEdgeValue(MinorType.VARDECIMAL, MinorType.FLOAT8, 1_000f)
    .putEdgeValue(MinorType.VARDECIMAL, MinorType.FLOAT4, 1_001f)
    // prefer the casts in the opposite directions
    .putEdgeValue(MinorType.VARDECIMAL, MinorType.INT, 1_002f)
    .putEdgeValue(MinorType.VARDECIMAL, MinorType.VARCHAR, 1_003f)

    // interval widening
    .putEdgeValue(MinorType.INTERVALDAY, MinorType.INTERVALYEAR, 10f)
    .putEdgeValue(MinorType.INTERVALYEAR, MinorType.INTERVAL, 10f)
    // interval conversions
    // prefer the cast in the opposite direction
    .putEdgeValue(MinorType.INTERVAL, MinorType.VARCHAR, 100f)

    // dict widening
    .putEdgeValue(MinorType.DICT, MinorType.MAP, 10f)

    // timestamp widening
    .putEdgeValue(MinorType.DATE, MinorType.TIMESTAMP, 10f)
    .putEdgeValue(MinorType.TIMESTAMP, MinorType.TIMESTAMPTZ, 10f)
    .putEdgeValue(MinorType.TIME, MinorType.TIMETZ, 10f)
    // timestamp conversions
    // prefer the casts in the opposite directions
    .putEdgeValue(MinorType.TIMESTAMP, MinorType.DATE, 100f)
    .putEdgeValue(MinorType.TIMESTAMP, MinorType.TIME, 101f)
    .putEdgeValue(MinorType.TIMESTAMPTZ, MinorType.VARCHAR, 1_000f)
    .putEdgeValue(MinorType.TIMETZ, MinorType.VARCHAR, 1_000f)

    // char and binary widening
    .putEdgeValue(MinorType.FIXEDCHAR, MinorType.VARCHAR, 10f)
    .putEdgeValue(MinorType.FIXEDBINARY, MinorType.VARBINARY, 10f)
    // char and binary conversions
    .putEdgeValue(MinorType.VARCHAR, MinorType.INT, 10f)
    .putEdgeValue(MinorType.VARCHAR, MinorType.FLOAT8, 20f)
    .putEdgeValue(MinorType.VARCHAR, MinorType.FLOAT4, 21f)
    .putEdgeValue(MinorType.VARCHAR, MinorType.VARDECIMAL, 30f)
    .putEdgeValue(MinorType.VARCHAR, MinorType.TIMESTAMP, 40f)
    .putEdgeValue(MinorType.VARCHAR, MinorType.INTERVALDAY, 50f)
    .putEdgeValue(MinorType.VARCHAR, MinorType.BIT, 60f)
    .putEdgeValue(MinorType.VARCHAR, MinorType.VARBINARY, 70f)
    .putEdgeValue(MinorType.VARBINARY, MinorType.VARCHAR, 80f)

    // union type sink vertex
    .putEdgeValue(MinorType.LIST, MinorType.UNION, 10f)
    .putEdgeValue(MinorType.MAP, MinorType.UNION, 10f)
    .putEdgeValue(MinorType.VARBINARY, MinorType.UNION, 10f)
    .putEdgeValue(MinorType.UNION, MinorType.LATE, 10f)

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
