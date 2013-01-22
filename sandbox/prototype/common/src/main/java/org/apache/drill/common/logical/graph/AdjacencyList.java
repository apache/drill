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

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimaps;


public class AdjacencyList<N extends Node<?>> {
  private Set<N> allNodes = new HashSet<N>();
  private ListMultimap<N, Edge<N>> adjacencies = ArrayListMultimap.create();

  public void addEdge(N source, N target, int weight) {
    adjacencies.put(source, new Edge<N>(source, target, weight));
    allNodes.add(source);
    allNodes.add(target);
  }

  public void clearVisited(){
    for (Edge<N> e : adjacencies.values()) {
      e.from.visited = false;
      e.to.visited = false;
    }
  }
  
  public List<Edge<N>> getAdjacent(N source) {
    return adjacencies.get(source);
  }

  
  public void printEdges(){
    for (Edge<N> e : adjacencies.values()) {
      System.out.println(e.from.index + " -> " + e.to.index);
    }
  }
  
  
//  public void reverseEdge(Edge<N> e) {
//    adjacencies.get(e.from).remove(e);
//    addEdge(e.to, e.from, e.weight);
//  }

//  public void reverseGraph() {
//    adjacencies = getReversedList().adjacencies;
//  }

  public AdjacencyList<N> getReversedList() {
    AdjacencyList<N> newlist = new AdjacencyList<N>();
    for (Edge<N> e : adjacencies.values()) {
      newlist.addEdge(e.to, e.from, e.weight);
    }
    return newlist;
  }

  public Set<N> getNodeSet() {
    return adjacencies.keySet();
  }

  /**
   * Get a list of nodes that have no outbound edges.
   * @return
   */
  public Collection<N> getTerminalNodes(){
    // we have to use the allNodes list as otherwise destination only nodes won't be found.
    List<N> nodes = new LinkedList<N>(allNodes);
    
    for(Iterator<N> i = nodes.iterator(); i.hasNext(); ){
      final N n = i.next();
      
      // remove any nodes that have one or more outbound edges.
      List<Edge<N>> adjList = this.getAdjacent(n);
      if(adjList != null && !adjList.isEmpty()) i.remove();
     
    }
    return nodes;
  }
  
  /**
   * Get a list of all nodes that have no incoming edges.
   * @return
   */
  public Collection<N> getStartNodes(){
    Set<N> nodes = new HashSet<N>(getNodeSet());
    for(Edge<N> e : adjacencies.values()){
      nodes.remove(e.to);
    }
    return nodes;
  }
  
  public Collection<Edge<N>> getAllEdges() {
    return adjacencies.values();
  }
  
  
  public void fix(){
    adjacencies = Multimaps.unmodifiableListMultimap(adjacencies);
    allNodes =  Collections.unmodifiableSet(allNodes);
  }
}
