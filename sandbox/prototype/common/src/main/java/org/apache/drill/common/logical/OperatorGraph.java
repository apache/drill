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
package org.apache.drill.common.logical;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.drill.common.expression.visitors.OpVisitor;
import org.apache.drill.common.logical.data.LogicalOperator;
import org.apache.drill.common.logical.data.SinkOperator;
import org.apache.drill.common.logical.data.SourceOperator;
import org.apache.drill.common.logical.graph.AdjacencyList;
import org.apache.drill.common.logical.graph.GraphAlgos;
import org.apache.drill.common.logical.graph.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OperatorGraph {

  static final Logger logger = LoggerFactory.getLogger(OperatorGraph.class);

  private AdjacencyList<OpNode> adjList;
  private final Collection<SourceOperator> sources;
  private final Collection<SinkOperator> sinks;

  public OperatorGraph(List<LogicalOperator> operators) {
    AdjacencyListBuilder b = new AdjacencyListBuilder();

    // Some of these operators are operator chains hidden through the use of sequences. This is okay because the
    // adjacency list builder is responsible for grabbing these as well.
    for (LogicalOperator o : operators) {
      o.accept(b);
    }

    adjList = b.getAdjacencyList();

     List<List<OpNode>> cyclicReferences = GraphAlgos.checkDirected(adjList);
     if(cyclicReferences.size() > 0){
     throw new
     IllegalArgumentException("A logical plan must be a valid DAG.  You have cyclic references in your graph.  " +
     cyclicReferences);
     }
    sources = convert(adjList.getStartNodes(), SourceOperator.class, "Error determing list of source operators.");
    // logger.debug("Source list {}", sources);
    sinks = convert(adjList.getTerminalNodes(), SinkOperator.class, "Error determing list of source operators.");
    // logger.debug("Sink list {}", sinks);

  }

  public AdjacencyList<OpNode> getAdjList() {
    return adjList;
  }

  public Collection<SourceOperator> getSources() {
    return sources;
  }

  public Collection<SinkOperator> getSinks() {
    return sinks;
  }

  @SuppressWarnings("unchecked")
  private <T extends LogicalOperator> Collection<T> convert(Collection<OpNode> nodes, Class<T> classIdentifier,
      String error) {
    List<T> out = new ArrayList<T>(nodes.size());
    for (OpNode o : nodes) {
      LogicalOperator lo = o.getNodeValue();
      if (classIdentifier.isAssignableFrom(lo.getClass())) {
        out.add((T) lo);
      } else {
        throw new UnexpectedOperatorType(classIdentifier, lo, error);
      }
    }
    return out;
  }

  public class AdjacencyListBuilder implements OpVisitor {
    Map<LogicalOperator, OpNode> ops = new HashMap<LogicalOperator, OpNode>();

    public boolean enter(LogicalOperator o) {
      visit(o);
      return true;
    }

    @Override
    public void leave(LogicalOperator o) {
//      for (LogicalOperator child : o) {
//        child.accept(this);
//      }
    }

    @Override
    public boolean visit(LogicalOperator o) {
      if(o == null) throw new IllegalArgumentException("Null operator.");
      
      if (!ops.containsKey(o)) {
        ops.put(o, new OpNode(o));
        return true;
      }

      return true;
    }

    public AdjacencyList<OpNode> getAdjacencyList() {
      logger.debug("Values; {}", ops.values().toArray());
      AdjacencyList<OpNode> a = new AdjacencyList<OpNode>();
      for (OpNode from : ops.values()) {
        for (LogicalOperator t : from.getNodeValue()) {
          OpNode to = ops.get(t);
          a.addEdge(from, to, 0);
        }

      }
      a.fix();
      return a;
    }

  }

  public static class OpNode extends Node<LogicalOperator> {

    public OpNode(LogicalOperator operator) {
      super(operator);
    }
  }

}
