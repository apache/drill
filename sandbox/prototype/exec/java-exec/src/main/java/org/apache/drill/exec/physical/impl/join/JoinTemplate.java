package org.apache.drill.exec.physical.impl.join;

import org.apache.drill.exec.record.RecordBatch;
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
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(JoinTemplate.class);

  @Override
  public void setupJoin(JoinStatus status, VectorContainer outgoing){
    
  }

  /**
   * Copy rows from the input record batches until the output record batch is full
   * @param status  State of the join operation (persists across multiple record batches/schema changes)
   */
  public final void doJoin(final JoinStatus status) {
    while (true) {
      // for each record

      // validate position and advance to the next record batch if necessary
      if (!status.isLeftPositionAllowed()) return;
      if (!status.isRightPositionAllowed()) return;

      int comparison = compare(status.leftPosition, status.rightPosition);
      switch (comparison) {

      case -1:
        // left key < right key
        status.advanceLeft();
        continue;

      case 0:
        // left key == right key
        if (!status.isLeftRepeating() &&
            status.isNextLeftPositionInCurrentBatch() &&
            compareNextLeftKey(status.leftPosition) == 0) {
          // records in the left batch contain duplicate keys
          // TODO: leftHasDups = true, if next left key matches but is in a new batch
          status.notifyLeftRepeating();
        }
        
        do {
          // copy all equal right keys to the output record batch
          if (!copy(status.leftPosition, status.rightPosition, status.outputPosition++))
            return;

          // If the left key has duplicates and we're about to cross batch boundaries, queue the
          // right table's record batch before calling next.  These records will need to be copied
          // again for each duplicate left key.
          if (status.isLeftRepeating() && !status.isNextRightPositionInCurrentBatch()) {
            // last record in right batch is a duplicate, and at the end of the batch
            status.outputBatch.addRightToBatchBuilder();
          }
          status.advanceRight();
        } while (status.isRightPositionAllowed() && compare(status.leftPosition, status.rightPosition) == 0);

        status.advanceLeft();

        if (status.isLeftRepeating() && compareNextLeftKey(status.leftPosition) != 0) {
          // left no longer has duplicates.  switch back to incoming batch mode
          status.setDefaultAdvanceMode();
          status.notifyLeftStoppedRepeating();
        } else if (status.isLeftRepeating()) {
          // left is going to repeat; use sv4 for right batch
          status.setRepeatedAdvanceMode();
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

  
  /**
   * Copy the data to the new record batch (if it fits).
   *
   * @param leftPosition  position of batch (lower 16 bits) and record (upper 16 bits) in left SV4
   * @param rightPosition position of batch (lower 16 bits) and record (upper 16 bits) in right SV4
   * @param outputPosition position of the output record batch
   * @return Whether or not the data was copied.
   */
  protected abstract boolean copy(int leftPosition, int rightPosition, int outputPosition);
  
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
  protected abstract int compare(int leftPosition, int rightPosition);
  protected abstract int compareNextLeftKey(int position);
  public abstract void setup(RecordBatch left, RecordBatch right, RecordBatch outgoing);
}
