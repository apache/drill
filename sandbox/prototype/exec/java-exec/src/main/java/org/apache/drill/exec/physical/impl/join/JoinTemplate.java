package org.apache.drill.exec.physical.impl.join;

import javax.inject.Named;

import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.record.VectorContainer;

/**
 * This join template uses a merge join to combine two ordered streams into a single larger batch.  When joining
 * single values on each side, the values can be copied to the outgoing batch immediately.  The outgoing record batch
 * should be sent as needed (e.g. schema change or outgoing batch full).  When joining multiple values on one or
 * both sides, two passes over the vectors will be made; one to construct the selection vector, and another to
 * generate the outgoing batches once the duplicate value is no longer encountered.
 *
 * Given two tables ordered by 'col1':
 *
 *        t1                t2
 *  ---------------   ---------------
 *  | key | col2 |    | key | col2 |
 *  ---------------   ---------------
 *  |  1  | 'ab' |    |  1  | 'AB' |
 *  |  2  | 'cd' |    |  2  | 'CD' |
 *  |  2  | 'ef' |    |  4  | 'EF' |
 *  |  4  | 'gh' |    |  4  | 'GH' |
 *  |  4  | 'ij' |    |  5  | 'IJ' |
 *  ---------------   ---------------
 *
 * 'SELECT * FROM t1 INNER JOIN t2 on (t1.key == t2.key)' should generate the following:
 *
 * ---------------------------------
 * | t1.key | t2.key | col1 | col2 |
 * ---------------------------------
 * |   1    |   1    | 'ab' | 'AB' |
 * |   2    |   2    | 'cd' | 'CD' |
 * |   2    |   2    | 'ef' | 'CD' |
 * |   4    |   4    | 'gh' | 'EF' |
 * |   4    |   4    | 'gh' | 'GH' |
 * |   4    |   4    | 'ij' | 'EF' |
 * |   4    |   4    | 'ij' | 'GH' |
 * ---------------------------------
 *
 * In the simple match case, only one row from each table matches.  Additional cases should be considered:
 *   - a left join key matches multiple right join keys
 *   - duplicate keys which may span multiple record batches (on the left and/or right side)
 *   - one or both incoming record batches change schemas
 *
 * In the case where a left join key matches multiple right join keys:
 *   - add a reference to all of the right table's matching values to the SV4.
 *
 * A RecordBatchData object should be used to hold onto all batches which have not been sent.
 *
 * JoinStatus:
 *   - all state related to the join operation is stored in the JoinStatus object.
 *   - this is required since code may be regenerated before completion of an outgoing record batch.
 */
public abstract class JoinTemplate implements JoinWorker {

  @Override
  public void setupJoin(FragmentContext context, JoinStatus status, VectorContainer outgoing) throws SchemaChangeException {
    doSetup(context, status, outgoing);
  }

  /**
   * Copy rows from the input record batches until the output record batch is full
   * @param status  State of the join operation (persists across multiple record batches/schema changes)
   */
  public final void doJoin(final JoinStatus status) {
    while (true) {
      // for each record

      // validate input iterators (advancing to the next record batch if necessary)
      if (!status.isRightPositionAllowed()) {
        // we've hit the end of the right record batch; copy any remaining values from the left batch
        while (status.isLeftPositionAllowed()) {
          doCopyLeft(status.getLeftPosition(), status.fetchAndIncOutputPos());
          status.advanceLeft();
        }
        return;
      }
      if (!status.isLeftPositionAllowed())
        return;

      int comparison = doCompare(status.getLeftPosition(), status.getRightPosition());
      switch (comparison) {

      case -1:
        // left key < right key
        doCopyLeft(status.getLeftPosition(), status.fetchAndIncOutputPos());
        status.advanceLeft();
        continue;

      case 0:
        // left key == right key

        // check for repeating values on the left side
        if (!status.isLeftRepeating() &&
            status.isNextLeftPositionInCurrentBatch() &&
            doCompareNextLeftKey(status.getLeftPosition()) == 0)
          // subsequent record(s) in the left batch have the same key
          status.notifyLeftRepeating();

        else if (status.isLeftRepeating() &&
                 status.isNextLeftPositionInCurrentBatch() &&
                 doCompareNextLeftKey(status.getLeftPosition()) != 0)
          // this record marks the end of repeated keys
          status.notifyLeftStoppedRepeating();
        
        boolean crossedBatchBoundaries = false;
        int initialRightPosition = status.getRightPosition();
        do {
          // copy all equal right keys to the output record batch
          if (!doCopyLeft(status.getLeftPosition(), status.getOutPosition()))
            return;

          if (!doCopyRight(status.getRightPosition(), status.fetchAndIncOutputPos()))
            return;
          
          // If the left key has duplicates and we're about to cross a boundary in the right batch, add the
          // right table's record batch to the sv4 builder before calling next.  These records will need to be
          // copied again for each duplicate left key.
          if (status.isLeftRepeating() && !status.isNextRightPositionInCurrentBatch()) {
            status.outputBatch.addRightToBatchBuilder();
            crossedBatchBoundaries = true;
          }
          status.advanceRight();

        } while (status.isRightPositionInCurrentBatch() && doCompare(status.getLeftPosition(), status.getRightPosition()) == 0);

        if (status.getRightPosition() > initialRightPosition && status.isLeftRepeating())
          // more than one matching result from right table; reset position in case of subsequent left match
          status.setRightPosition(initialRightPosition);
        status.advanceLeft();

        if (status.isLeftRepeating() && doCompareNextLeftKey(status.getLeftPosition()) != 0) {
          // left no longer has duplicates.  switch back to incoming batch mode
          status.setDefaultAdvanceMode();
          status.notifyLeftStoppedRepeating();
        } else if (status.isLeftRepeating() && crossedBatchBoundaries) {
          try {
            // build the right batches and 
            status.outputBatch.batchBuilder.build();
            status.setSV4AdvanceMode();
          } catch (SchemaChangeException e) {
            status.ok = false;
          }
          // return to indicate recompile in right-sv4 mode
          return;
        }

        continue;

      case 1:
        // left key > right key
        status.advanceRight();
        continue;

      default:
        throw new IllegalStateException();
      }
    }
  }

  // Generated Methods

  public abstract void doSetup(@Named("context") FragmentContext context,
      @Named("status") JoinStatus status,
      @Named("outgoing") VectorContainer outgoing) throws SchemaChangeException;


  /**
   * Copy the data to the new record batch (if it fits).
   *
   * @param leftPosition  position of batch (lower 16 bits) and record (upper 16 bits) in left SV4
   * @param outputPosition position of the output record batch
   * @return Whether or not the data was copied.
   */
  public abstract boolean doCopyLeft(@Named("leftIndex") int leftIndex, @Named("outIndex") int outIndex);
  public abstract boolean doCopyRight(@Named("rightIndex") int rightIndex, @Named("outIndex") int outIndex);


  /**
   * Compare the values of the left and right join key to determine whether the left is less than, greater than
   * or equal to the right.
   *
   * @param leftPosition
   * @param rightPosition
   * @return  0 if both keys are equal
   *         -1 if left is < right
   *          1 if left is > right
   */
  protected abstract int doCompare(@Named("leftIndex") int leftIndex,
      @Named("rightIndex") int rightIndex);


  /**
   * Compare the current left key to the next left key, if it's within the batch.
   * @return  0 if both keys are equal
   *          1 if the keys are not equal
   *         -1 if there are no more keys in this batch
   */
  protected abstract int doCompareNextLeftKey(@Named("leftIndex") int leftIndex);


}
