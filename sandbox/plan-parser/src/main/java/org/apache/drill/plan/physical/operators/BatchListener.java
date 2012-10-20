package org.apache.drill.plan.physical.operators;

/**
 * Aggregate and Implode operators need to know when batches of records are finished and thus
 * they implement BatchListener.  Note that the source of data is different from the source of
 * batch boundaries. This avoids the need for every data processor to propagate boundaries but
 * it also allows fancy structures to be constructed to do interesting things.
 */
public interface BatchListener {
    public void endBatch(Object parent);
}
