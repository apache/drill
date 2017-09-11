
package org.apache.drill.exec.test.generated;

import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.impl.project.ProjectorTemplate;
import org.apache.drill.exec.record.RecordBatch;

public class ProjectorGen0
    extends ProjectorTemplate
{


    public ProjectorGen0() {
        try {
            __DRILL_INIT__();
        } catch (SchemaChangeException e) {
            throw new UnsupportedOperationException(e);
        }
    }

    public void doEval(int inIndex, int outIndex)
        throws SchemaChangeException
    {
    }

    public void doSetup(FragmentContext context, RecordBatch incoming, RecordBatch outgoing)
        throws SchemaChangeException
    {
    }

    public void __DRILL_INIT__()
        throws SchemaChangeException
    {
    }

}
