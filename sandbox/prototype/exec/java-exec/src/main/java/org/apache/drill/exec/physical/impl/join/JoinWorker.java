package org.apache.drill.exec.physical.impl.join;

import org.apache.drill.exec.compile.TemplateClassDefinition;
import org.apache.drill.exec.record.VectorContainer;


public interface JoinWorker {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(JoinWorker.class);
  
  public static enum JoinOutcome {
    NO_MORE_DATA, BATCH_RETURNED, MODE_CHANGED, SCHEMA_CHANGED, WAITING, FAILURE;
  }
  
  public static TemplateClassDefinition<JoinWorker> TEMPLATE_DEFINITION = new TemplateClassDefinition<JoinWorker>( //
      JoinWorker.class, "org.apache.drill.exec.physical.impl.mergejoin.JoinTemplate", JoinEvaluator.class, null);

  
  public void setupJoin(JoinStatus status, VectorContainer outgoing);
  public void doJoin(JoinStatus status);
}
