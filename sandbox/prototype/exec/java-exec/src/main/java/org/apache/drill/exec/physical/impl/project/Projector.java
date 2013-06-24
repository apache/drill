package org.apache.drill.exec.physical.impl.project;

import java.util.List;

import org.apache.drill.exec.compile.TemplateClassDefinition;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.record.RecordBatch;

public interface Projector {

  public abstract void setup(FragmentContext context, RecordBatch incoming, List<TransferPairing<?>> transfers)  throws SchemaChangeException;

  
  public abstract void projectRecords(int recordCount, int firstOutputIndex);

  public static TemplateClassDefinition<Projector, Void> TEMPLATE_DEFINITION = new TemplateClassDefinition<Projector, Void>( //
      Projector.class, "org.apache.drill.exec.physical.impl.project.ProjectTemplate", ProjectEvaluator.class, Void.class);

}