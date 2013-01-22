package org.apache.drill.exec.ref.rops;

import java.util.BitSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.drill.exec.ref.RecordPointer;
import org.apache.drill.exec.ref.eval.EvaluatorTypes.BasicEvaluator;
import org.apache.drill.exec.ref.values.DataValue;
import org.apache.drill.exec.ref.values.ScalarValues.BytesScalar;

/**
 * MultiLevel map is a map of maps of maps (etc>. Each map is keyed by a data value. At the lowest level, the value is a
 * list or record as opposed to a another straight map. A 4 way group by clause will therefore effectively look like
 * Map<group1, Map<group2, Map<group3, Map<group4, record>>>>.  
 */
public class MultiLevelMap {
  private BasicEvaluator[] fields;
  private final int deepestLevel;
  private final Level rootLevel;

  public MultiLevelMap(BasicEvaluator[] fields) {
    if (fields.length > 8)
      throw new UnsupportedOperationException("Only groupings to eight levels are currently supported.");
    this.fields = fields;
    deepestLevel = fields.length - 1;
    rootLevel = fields.length == 1 ? new LastLevel(fields[0]) : new IntermediateLevel(fields[0]);
  }

  public void add(RecordPointer r) {
    rootLevel.add(r, 0);
  }

  private class IntermediateLevel implements Level {
    final BasicEvaluator grouping;
    Map<DataValue, Level> levelMap = new HashMap<DataValue, Level>();

    public IntermediateLevel(BasicEvaluator field) {
      super();
      this.grouping = field;
    }

    @Override
    public void add(RecordPointer r, int level) {
      DataValue v = grouping.eval();
      if (v == null) v = DataValue.NULL_VALUE;
      Level l = levelMap.get(v);

      level++;
      
      if (l == null) {
        l = (level == deepestLevel) ? new LastLevel(fields[level]) :  new IntermediateLevel(fields[level]);
        levelMap.put(v, l);
      }

      l.add(r, level);
    }

    @Override
    public Iterator<?> iter() {
      return levelMap.entrySet().iterator();
    }

  }

  private class LastLevel implements Level {
    private final BasicEvaluator grouping;
    Map<DataValue, List<RecordPointer>> recordMap = new HashMap<DataValue, List<RecordPointer>>();

    public LastLevel(BasicEvaluator grouping) {
      super();
      this.grouping = grouping;
    }

    @Override
    public void add(RecordPointer r, int level) {
      DataValue dv = grouping.eval();
      if (dv == null) dv = DataValue.NULL_VALUE;
      List<RecordPointer> list = recordMap.get(dv);
      if (list == null) {
        list = new LinkedList<RecordPointer>();
        recordMap.put(dv, list);
      }
      list.add(r);

    }

    @Override
    public Iterator<?> iter() {
      return recordMap.entrySet().iterator();
    }
  }

  public MultiLevelIterator getIterator() {
    return new MultiLevelIterator();
  }

  /**
   * Iterator for working through a MultiLevel map. The iterator only works if every sub map has at least one entry.
   * This should always be the case.
   */
  public class MultiLevelIterator {
    private final GroupingEntry ge;
    private final Iterator<?>[] iterators;
    private final DataValue[] values;
    private Iterator<RecordPointer> lowestLevelRecordIterator = null;

    private MultiLevelIterator() {
      this.iterators = new Iterator[fields.length];
      this.values = new DataValue[fields.length];
      this.ge = new GroupingEntry(values);
      iterators[0] = rootLevel.iter();

    }

    public GroupingEntry next() {
      if (lowestLevelRecordIterator != null && lowestLevelRecordIterator.hasNext()) {
        ge.record = lowestLevelRecordIterator.next();
        return ge;
      }

      ge.bitset.clear();
      int level = deepestLevel;

      // pop empty iterators.
      for (; level > 0; level--) {

        // if the current iterator is null or has no next, we'll need it replaced.
        if ((iterators[level] == null) || !iterators[level].hasNext()) {
          // set change bit
          ge.bitset.set(level);
          continue;
        } else {
          break;
        }
      }

      // if we're at the top level and the current iterator doesn't have any more items, we're done iterating.
      if (level == 0) {
        if (!iterators[0].hasNext()) return null;
        ge.bitset.set(0);
      }

      // go through all but the bottom most level.
      for (int i = level; i < deepestLevel; i++) {
        int nextLevel = i + 1;
        Iterator<?> iter = iterators[i];

        @SuppressWarnings("unchecked")
        Map.Entry<DataValue, Level> e = (Entry<DataValue, Level>) iter.next();
        values[nextLevel] = e.getKey();
        iterators[nextLevel] = e.getValue().iter();
      }

      @SuppressWarnings("unchecked")
      Entry<DataValue, List<RecordPointer>> lowest = (Entry<DataValue, List<RecordPointer>>) iterators[deepestLevel]
          .next();
      lowestLevelRecordIterator = lowest.getValue().iterator();
      ge.record = lowestLevelRecordIterator.next();
      return ge;
    }

  }

  /**
   * A grouping entry that gives you the value for each of the grouping keys. Changebits informs you of whether
   * particular nested groups are restarting.
   */
  public class GroupingEntry {
    public final BasicEvaluator[] keys;
    public final DataValue[] values;
    public final BitSet bitset;
    public RecordPointer record;
    private BytesScalar groupKey;

    public GroupingEntry(DataValue[] values) {
      super();
      this.groupKey = new BytesScalar(new byte[1]);
      this.values = values;
      this.keys = MultiLevelMap.this.fields;
      bitset = new BitSet(keys.length);
    }

    public BytesScalar getGroupKey() {
      this.groupKey = new BytesScalar(bitset.toByteArray());
      return groupKey;
    }

  }

  public interface Level {
    public void add(RecordPointer r, int level);

    public Iterator<?> iter();
  }
}
