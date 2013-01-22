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
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GraphAlgos {
  static final Logger logger = LoggerFactory.getLogger(GraphAlgos.class);

  public static class TopoSorter<N extends Node<?>> {
    final List<N> sorted = new LinkedList<N>();
    final AdjacencyList<N> rGraph;

    private TopoSorter(AdjacencyList<N> graph) {
      graph.clearVisited();
      
      this.rGraph = graph.getReversedList();
      Collection<N> sourceNodes = rGraph.getStartNodes();

      for (N n : sourceNodes) {
        visit(n);
      }
    }

    private void visit(N n) {
      if (n.visited)
        return;

      n.visited = true;
      List<Edge<N>> edges = rGraph.getAdjacent(n);
      if (edges != null) {
        for (Edge<N> e : edges) {
          visit(e.to);
        }
      }

      sorted.add(n);

    }

    /**
     * Execute a depth-first sort on the reversed DAG.
     * 
     * @param graph
     *          The adjacency list for the DAG.
     * @param sourceNodes
     *          List of nodes that
     * @return
     */
    public static <N extends Node<?>> List<N> sort(AdjacencyList<N> graph) {
      TopoSorter<N> ts = new TopoSorter<N>(graph);
      return ts.sorted;
    }
  }

  public static <N extends Node<?>> List<List<N>> checkDirected(AdjacencyList<N> graph) {
    Tarjan<N> t = new Tarjan<N>();
    List<List<N>> subgraphs = t.executeTarjan(graph);
    for (Iterator<List<N>> i = subgraphs.iterator(); i.hasNext();) {
      List<N> l = i.next();
      if (l.size() == 1)  i.remove();
    }
    return subgraphs;
  }

  public static class Tarjan<N extends Node<?>> {

    private int index = 0;
    private List<N> stack = new LinkedList<N>();
    private List<List<N>> SCC = new LinkedList<List<N>>();

    public List<List<N>> executeTarjan(AdjacencyList<N> graph) {
      SCC.clear();
      index = 0;
      stack.clear();
      if (graph != null) {
        List<N> nodeList = new LinkedList<N>(graph.getNodeSet());
        for (N node : nodeList) {
          if (node.index == -1) {
            tarjan(node, graph);
          }
        }
      }
      return SCC;
    }

    private List<List<N>> tarjan(N v, AdjacencyList<N> list) {
      v.index = index;
      v.lowlink = index;
      index++;
      stack.add(0, v);
      List<Edge<N>> l = list.getAdjacent(v);
      if (l != null) {
        for (Edge<N> e : l) {
          N n = e.to;
          if (n.index == -1) {
            tarjan(n, list);
            v.lowlink = Math.min(v.lowlink, n.lowlink);
          } else if (stack.contains(n)) {
            v.lowlink = Math.min(v.lowlink, n.index);
          }
        }
      }
      if (v.lowlink == v.index) {
        N n;
        List<N> component = new LinkedList<N>();
        do {
          n = stack.remove(0);
          component.add(n);
        } while (n != v);
        SCC.add(component);
      }
      return SCC;
    }
  }
}
