package org.apache.drill.exec.physical.impl.sort;

import org.apache.drill.exec.compile.TemplateClassDefinition;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.selection.SelectionVector4;

public interface Sorter {
  public void setup(FragmentContext context, RecordBatch hyperBatch) throws SchemaChangeException;
  public void sort(SelectionVector4 vector4, VectorContainer container);
  
  public static TemplateClassDefinition<Sorter> TEMPLATE_DEFINITION = new TemplateClassDefinition<Sorter>( //
      Sorter.class, "org.apache.drill.exec.physical.impl.sort.SortTemplate", Comparator.class, SortSignature.class);

}
