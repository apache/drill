package org.apache.drill.exec.physical.impl.filter;

import org.apache.drill.exec.compile.TemplateClassDefinition;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.impl.project.Projector;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.TransferPair;

public interface Filterer {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Filterer.class);
  
  public void setup(FragmentContext context, RecordBatch incoming, RecordBatch outgoing, TransferPair[] transfers) throws SchemaChangeException;
  public void filterBatch(int recordCount);
  
  public static TemplateClassDefinition<Filterer> TEMPLATE_DEFINITION = new TemplateClassDefinition<Filterer>( //
      Filterer.class, "org.apache.drill.exec.physical.impl.filter.FilterTemplate", FilterEvaluator.class, boolean.class);

}
