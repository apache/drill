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
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableSet;
import org.apache.drill.shaded.guava.com.google.common.graph.ImmutableValueGraph;
import org.apache.drill.shaded.guava.com.google.common.graph.ValueGraphBuilder;

public class ResolverTypePrecedence {

  // Data types that are defined but not currently supported in Drill.
  public static final Set<MinorType> UNSUPPORTED_TYPES = ImmutableSet.of(
    MinorType.TINYINT,
    MinorType.SMALLINT,
    MinorType.UINT1,
    MinorType.UINT2
  );

  // Casting cost values are orders of magnitude intended only to capture that
  // one cast is cheaper or less risky than another. They do not try to reflect
  // any accurate information about how much a cast costs computationally.

  // Cost of casting between primitive types
  public static final float PRIMITIVE_TYPE_COST = 1f;
  // Base cost of casting
  public static final float BASE_COST = 10f;
  // Cost of casting with the possibility of a loss of precision.
  // Chosen to definitely be larger than any path of BASE_COST
  // edges across the implicit casting graph.
  public static final float PRECISION_LOSS_COST = 1000f;

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
    .putEdgeValue(MinorType.NULL, MinorType.MONEY, BASE_COST)
    // special case: make INT the nearest cast for NULL
    .putEdgeValue(MinorType.NULL, MinorType.INT, 0.5f*PRIMITIVE_TYPE_COST)

    // unsigned int widening
    .putEdgeValue(MinorType.BIT, MinorType.UINT1, PRIMITIVE_TYPE_COST)
    .putEdgeValue(MinorType.UINT1, MinorType.UINT2, PRIMITIVE_TYPE_COST)
    .putEdgeValue(MinorType.UINT2, MinorType.UINT4, PRIMITIVE_TYPE_COST)
    .putEdgeValue(MinorType.UINT4, MinorType.UINT8, PRIMITIVE_TYPE_COST)
    .putEdgeValue(MinorType.UINT8, MinorType.VARDECIMAL, BASE_COST)
    // unsigned int conversions
    .putEdgeValue(MinorType.UINT4, MinorType.BIGINT, PRIMITIVE_TYPE_COST)
    .putEdgeValue(MinorType.UINT4, MinorType.FLOAT8, 2*PRIMITIVE_TYPE_COST)

    // int widening
    .putEdgeValue(MinorType.BIT, MinorType.TINYINT, PRIMITIVE_TYPE_COST)
    .putEdgeValue(MinorType.TINYINT, MinorType.SMALLINT, PRIMITIVE_TYPE_COST)
    .putEdgeValue(MinorType.SMALLINT, MinorType.INT, PRIMITIVE_TYPE_COST)
    .putEdgeValue(MinorType.INT, MinorType.BIGINT, PRIMITIVE_TYPE_COST)
    .putEdgeValue(MinorType.BIGINT, MinorType.VARDECIMAL, BASE_COST)
    // int conversions
    .putEdgeValue(MinorType.INT, MinorType.FLOAT8, 2*PRIMITIVE_TYPE_COST)
    .putEdgeValue(MinorType.BIGINT, MinorType.FLOAT8, PRECISION_LOSS_COST)

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
    // decimal conversions
    .putEdgeValue(MinorType.VARDECIMAL, MinorType.FLOAT8, 0.5f*BASE_COST)
    .putEdgeValue(MinorType.VARDECIMAL, MinorType.VARCHAR, BASE_COST)

    // interval widening
    .putEdgeValue(MinorType.INTERVALDAY, MinorType.INTERVALYEAR, BASE_COST)
    .putEdgeValue(MinorType.INTERVALYEAR, MinorType.INTERVAL, BASE_COST)
    // interval conversions
    .putEdgeValue(MinorType.INTERVAL, MinorType.VARCHAR, BASE_COST)

    // dict widening
    .putEdgeValue(MinorType.DICT, MinorType.MAP, BASE_COST)

    // timestamp widening
    .putEdgeValue(MinorType.DATE, MinorType.TIMESTAMP, BASE_COST)
    .putEdgeValue(MinorType.TIMESTAMP, MinorType.TIMESTAMPTZ, BASE_COST)
    .putEdgeValue(MinorType.TIME, MinorType.TIMETZ, BASE_COST)
    // timestamp conversions
    .putEdgeValue(MinorType.TIMESTAMP, MinorType.DATE, PRECISION_LOSS_COST)
    .putEdgeValue(MinorType.TIMESTAMP, MinorType.TIME, PRECISION_LOSS_COST)
    .putEdgeValue(MinorType.TIMESTAMP, MinorType.VARCHAR, BASE_COST)
    .putEdgeValue(MinorType.TIMETZ, MinorType.VARCHAR, BASE_COST)

    // char and binary widening
    .putEdgeValue(MinorType.FIXEDBINARY, MinorType.VARBINARY, BASE_COST)
    .putEdgeValue(MinorType.FIXEDCHAR, MinorType.VARCHAR, BASE_COST)
    // char and binary conversions
    .putEdgeValue(MinorType.VARCHAR, MinorType.VARDECIMAL, BASE_COST)
    .putEdgeValue(MinorType.VARCHAR, MinorType.INTERVALDAY, BASE_COST)
    .putEdgeValue(MinorType.VARCHAR, MinorType.DATE, BASE_COST)
    .putEdgeValue(MinorType.VARCHAR, MinorType.TIME, BASE_COST)
    .putEdgeValue(MinorType.VARCHAR, MinorType.INT, PRECISION_LOSS_COST)
    .putEdgeValue(MinorType.VARCHAR, MinorType.VARBINARY, BASE_COST)

    // union type sink vertex
    .putEdgeValue(MinorType.LIST, MinorType.UNION, BASE_COST)
    .putEdgeValue(MinorType.MAP, MinorType.UNION, BASE_COST)
    .putEdgeValue(MinorType.VARBINARY, MinorType.UNION, BASE_COST)

    .build();

  /**
   * Searches the implicit casting graph for the path of least total cost using
   * Dijkstra's algorithm. A return value greater than PRECISION_LOSS_COST means
   * that cast may involve a loss of precision.
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
