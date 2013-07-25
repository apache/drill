package org.apache.drill.exec.physical.impl.project;

import java.util.List;

import org.apache.drill.exec.compile.TemplateClassDefinition;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.TransferPair;

public interface Projector {

  public abstract void setup(FragmentContext context, RecordBatch incoming,  RecordBatch outgoing, List<TransferPair> transfers)  throws SchemaChangeException;

  
  public abstract int projectRecords(int recordCount, int firstOutputIndex);

  public static TemplateClassDefinition<Projector> TEMPLATE_DEFINITION = new TemplateClassDefinition<Projector>( //
      Projector.class, "org.apache.drill.exec.physical.impl.project.ProjectorTemplate", ProjectEvaluator.class, null);

}