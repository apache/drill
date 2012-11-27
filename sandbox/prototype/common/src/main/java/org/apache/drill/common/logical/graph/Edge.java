package org.apache.drill.common.logical.graph;


public class Edge<N> implements Comparable<Edge<N>> {

  final N from, to;
  final int weight;

  public Edge(final N argFrom, final N argTo, final int argWeight) {
    from = argFrom;
    to = argTo;
    weight = argWeight;
  }

  public int compareTo(final Edge<N> argEdge) {
    return weight - argEdge.weight;
  }

  @Override
  public String toString() {
    return "Edge [from=" + from + ", to=" + to + "]";
  }
  
  
}