package org.apache.drill.common.logical.graph;


public class Node<T> implements Comparable<Node<T>> {
  final T nodeValue;
  boolean visited = false; // used for Kosaraju's algorithm and Edmonds's
                           // algorithm
  int lowlink = -1; // used for Tarjan's algorithm
  int index = -1; // used for Tarjan's algorithm

  public Node(final T operator) {
    this.nodeValue = operator;
  }

  public int compareTo(final Node<T> argNode) {
    // just do an identity compare since elsewhere you should ensure that only one node exists for each nodeValue.
    return argNode == this ? 0 : -1;
  }
  
  @Override
  public int hashCode() {
    return nodeValue.hashCode(); 
  }

  public T getNodeValue(){
    return nodeValue;
  }

  @Override
  public String toString() {
    return "Node [val=" + nodeValue + "]";
  }

  
}