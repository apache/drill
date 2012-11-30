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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class AdjacencyList<N extends Node<?>> {
  private Set<N> allNodes = new HashSet<N>();
  private Map<N, List<Edge<N>>> adjacencies = new HashMap<N, List<Edge<N>>>();

  public void addEdge(N source, N target, int weight) {
    List<Edge<N>> list;
    if (!adjacencies.containsKey(source)) {
      list = new ArrayList<Edge<N>>();
      adjacencies.put(source, list);
    } else {
      list = adjacencies.get(source);
    }
    list.add(new Edge<N>(source, target, weight));
    allNodes.add(source);
    allNodes.add(target);
  }

  public List<Edge<N>> getAdjacent(N source) {
    return adjacencies.get(source);
  }

  public void reverseEdge(Edge<N> e) {
    adjacencies.get(e.from).remove(e);
    addEdge(e.to, e.from, e.weight);
  }

  public void reverseGraph() {
    adjacencies = getReversedList().adjacencies;
  }

  public AdjacencyList<N> getReversedList() {
    AdjacencyList<N> newlist = new AdjacencyList<N>();
    for (List<Edge<N>> edges : adjacencies.values()) {
      for (Edge<N> e : edges) {
        newlist.addEdge(e.to, e.from, e.weight);
      }
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
    for(List<Edge<N>> le : adjacencies.values()){
      for(Edge<N> e : le){
        nodes.remove(e.to);
      }
    }
    return nodes;
  }
  
  public Collection<Edge<N>> getAllEdges() {
    List<Edge<N>> edges = new LinkedList<Edge<N>>();
    for (List<Edge<N>> e : adjacencies.values()) {
      edges.addAll(e);
    }
    return edges;
  }
}
