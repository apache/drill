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
  
  public OperatorGraph(List<LogicalOperator> operators){
    AdjacencyListBuilder b = new AdjacencyListBuilder();
    for(LogicalOperator o : operators){
      o.accept(b);
    }
    
    adjList = b.getAdjacencyList();
    
    List<List<OpNode>> cyclicReferences = GraphAlgos.checkDirected(adjList);
    if(cyclicReferences.size() > 0){
      throw new IllegalArgumentException("A logical plan must be a valid DAG.  You have cyclic references in your graph.  " + cyclicReferences);
    }
    sources = convert(adjList.getStartNodes(), SourceOperator.class, "Error determing list of source operators.");
//    logger.debug("Source list {}", sources);
    sinks = convert(adjList.getTerminalNodes(), SinkOperator.class, "Error determing list of source operators.");
//    logger.debug("Sink list {}", sinks);
    
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
  private <T extends LogicalOperator> Collection<T> convert(Collection<OpNode> nodes, Class<T> classIdentifier, String error){
    List<T> out = new ArrayList<T>(nodes.size());
    for(OpNode o : nodes){
      LogicalOperator lo = o.getNodeValue();
      if(classIdentifier.isAssignableFrom(lo.getClass())){
        out.add( (T) lo);
      }else{
        throw new UnexpectedOperatorType(classIdentifier, lo, error);
      }
    }
    return out;
  }
  
  public class AdjacencyListBuilder implements OpVisitor{
    Map<LogicalOperator, OpNode> ops = new HashMap<LogicalOperator, OpNode>();
    
    @Override
    public void visit(LogicalOperator o) {
      if(!ops.containsKey(o)){
//        logger.debug("Adding node {}", o);
        ops.put(o,  new OpNode(o));
      }
    }
    
    public AdjacencyList<OpNode> getAdjacencyList(){
      AdjacencyList<OpNode> a = new AdjacencyList<OpNode>();
      for(OpNode from : ops.values()){
//        logger.debug("Adding edges for {}", from);
        for(LogicalOperator t : from.getNodeValue()){
//          logger.debug("\t\tAdding edges to {}", t);
          OpNode to = ops.get(t);
          a.addEdge(from, to, 0);
        }
        
      }
      return a;
    }
    
  }
  
  public static class OpNode extends Node<LogicalOperator>{

    public OpNode(LogicalOperator operator) {
      super(operator);
    }
  }
    
}
