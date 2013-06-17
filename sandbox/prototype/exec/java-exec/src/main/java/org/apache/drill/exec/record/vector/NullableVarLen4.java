package org.apache.drill.exec.record.vector;

import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.record.MaterializedField;

public class NullableVarLen4 extends NullableValueVector<NullableVarLen4, VarLen4> {

    public NullableVarLen4(MaterializedField field, BufferAllocator allocator) {
        super(field, allocator);
    }

    @Override
    protected VarLen4 getNewValueVector(BufferAllocator allocator) {
        return new VarLen4(field, allocator);
    }

    public void setBytes(int index, byte[] bytes) {
        setNotNull(index);
        value.setBytes(index, bytes);
    }
}
