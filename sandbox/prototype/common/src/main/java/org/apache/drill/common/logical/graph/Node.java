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
package org.apache.drill.common.logical.graph;


public class Node<T> implements Comparable<Node<T>> {
  final T nodeValue;
  boolean visited = false; // used for Kosaraju's algorithm and Edmonds's
                           // algorithm
  int lowlink = -1; // used for Tarjan's algorithm
  int index = -1; // used for Tarjan's algorithm

  public Node(final T operator) {
    if(operator == null) throw new IllegalArgumentException("Operator node was null.");
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
