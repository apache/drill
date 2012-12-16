package org.apache.drill.exec.store;

import org.apache.drill.common.exceptions.DrillRuntimeException;

public class BatchExceededException extends DrillRuntimeException {
    public BatchExceededException(int capacity, int attempted) {
        super("Batch exceeded in size. Capacity: " + capacity + ", Attempted: " + attempted);
    }
}
