package org.apache.drill.exec.physical.impl.project;

import java.util.List;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.Project;
import org.apache.drill.exec.physical.impl.BatchCreator;
import org.apache.drill.exec.record.RecordBatch;

import com.google.common.base.Preconditions;

public class ProjectBatchCreator implements BatchCreator<Project>{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ProjectBatchCreator.class);

  @Override
  public RecordBatch getBatch(FragmentContext context, Project config, List<RecordBatch> children) throws ExecutionSetupException {
    Preconditions.checkArgument(children.size() == 1);
    return new ProjectRecordBatch(config, children.iterator().next(), context);
  }
  
  
}
