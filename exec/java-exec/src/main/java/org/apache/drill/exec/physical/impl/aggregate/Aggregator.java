package org.apache.drill.exec.physical.impl.aggregate;

import org.apache.drill.exec.compile.TemplateClassDefinition;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.impl.aggregate.AggBatch.AggOutcome;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.RecordBatch.IterOutcome;
import org.apache.drill.exec.vector.allocator.VectorAllocator;

public interface Aggregator {

  public static TemplateClassDefinition<Aggregator> TEMPLATE_DEFINITION = new TemplateClassDefinition<Aggregator>(Aggregator.class, AggTemplate.class);
  
  public abstract void setup(FragmentContext context, RecordBatch incoming, RecordBatch outgoing,
      VectorAllocator[] allocators) throws SchemaChangeException;

  public abstract IterOutcome getOutcome();

  public abstract int getOutputCount();

  public abstract AggOutcome doWork();

  public abstract void cleanup();

}