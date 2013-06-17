package org.apache.drill.exec.record.vector;

import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.record.MaterializedField;

public class NullableBit extends NullableValueVector<NullableBit, Bit>{
    public NullableBit(MaterializedField field, BufferAllocator allocator) {
        super(field, allocator);
    }

    @Override
    protected Bit getNewValueVector(BufferAllocator allocator) {
        return new Bit(this.field, allocator);
    }

    public void set(int index) {
        setNotNull(index);
        value.set(index);
    }
}
