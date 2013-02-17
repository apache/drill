/*******************************************************************************
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package org.apache.drill.exec.ref.rops;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.logical.data.Join;
import org.apache.drill.common.logical.data.JoinCondition;
import org.apache.drill.common.logical.data.JoinType;
import org.apache.drill.exec.ref.IteratorRegistry;
import org.apache.drill.exec.ref.RecordIterator;
import org.apache.drill.exec.ref.RecordPointer;
import org.apache.drill.exec.ref.UnbackedRecord;
import org.apache.drill.exec.ref.eval.EvaluatorFactory;
import org.apache.drill.exec.ref.eval.fn.ComparisonEvaluators;
import org.apache.drill.exec.ref.exceptions.RecordException;
import org.apache.drill.exec.ref.exceptions.SetupException;
import org.apache.drill.exec.ref.values.ComparableValue;
import org.apache.drill.exec.ref.values.DataValue;

import java.util.List;

import static org.apache.drill.common.logical.data.Join.JoinType.LEFT;

public class JoinROP extends ROPBase<Join> {
    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(JoinROP.class);

    private RecordIterator left;
    private RecordIterator right;
    private RecordPointer record;
    private EvaluatorFactory factory;

    public JoinROP(Join config) {
        super(config);
        record = new UnbackedRecord();
    }

    @Override
    protected void setupIterators(IteratorRegistry builder) {
        left = Iterables.getOnlyElement(builder.getOperator(config.getLeft()));
        right = Iterables.getOnlyElement(builder.getOperator(config.getRight()));
    }

    @Override
    protected void setupEvals(EvaluatorFactory builder) throws SetupException {
        factory = builder;
    }

    @Override
    protected RecordIterator getIteratorInternal() {
        return createIteratorFromJoin(config.getJointType());
    }

    private RecordIterator createIteratorFromJoin(Join.JoinType type) {
        switch (type) {
            case INNER:
                return new InnerIterator();
            case OUTER:
                return new OuterIterator();
            case RIGHT:
                return new RightIterator();
            default:
                throw new UnsupportedOperationException("Type not supported: " + type);
        }
    }

    private class RecordBuffer {
        final boolean schemaChanged;
        final RecordPointer pointer;
        boolean hasJoined = false;

        private RecordBuffer(RecordPointer pointer, boolean schemaChanged) {
            this.pointer = pointer;
            this.schemaChanged = schemaChanged;
        }

        public void setHasJoined(boolean hasJoined) {
            this.hasJoined = hasJoined;
        }
    }

    abstract class JoinIterator implements RecordIterator {
        protected List<RecordBuffer> buffer;
        protected int curIdx = 0;
        protected int bufferLength = 0;

        protected abstract int setupBuffer();

        protected int setupBufferForIterator(RecordIterator iterator) {
            int count = 0;
            NextOutcome outcome = iterator.next();
            while (outcome != NextOutcome.NONE_LEFT) {
                buffer.add(new RecordBuffer(
                        iterator.getRecordPointer().copy(),
                        outcome == NextOutcome.INCREMENTED_SCHEMA_CHANGED)
                );
                ++count;
                outcome = iterator.next();
            }
            return count;
        }

        @Override
        public RecordPointer getRecordPointer() {
            return record;
        }

        public NextOutcome next() {
            if (buffer == null) {
                buffer = Lists.newArrayList();
                setupBuffer();
                bufferLength = buffer.size();
            }
            return getNext();
        }

        public abstract NextOutcome getNext();

        protected void setJoinedRecord(RecordPointer left, RecordPointer right) {
            if (left != null) {
                record.copyFrom(left);
            }
            if (right != null) {
                record.copyFrom(right);
            }
        }

        public boolean eval(DataValue leftVal, DataValue rightVal, String relationship) {
            // Skip join if no comparison can be made
            if (!ComparisonEvaluators.isComparable(leftVal, rightVal)) {
                return false;
            }

            //Somehow utilize ComparisonEvaluators?
            switch (relationship) {
                case "!=":
                    return !leftVal.equals(rightVal);
                case "==":
                    return leftVal.equals(rightVal);
                case "<":
                    return ((ComparableValue) leftVal).compareTo(rightVal) < 0;
                case "<=":
                    return ((ComparableValue) leftVal).compareTo(rightVal) <= 0;
                case ">":
                    return ((ComparableValue) leftVal).compareTo(rightVal) > 0;
                case ">=":
                    return ((ComparableValue) leftVal).compareTo(rightVal) >= 0;
                default:
                    throw new DrillRuntimeException("Relationship not supported: " + relationship);
            }
        }

        @Override
        public ROP getParent() {
            return JoinROP.this;
        }
    }

    class InnerIterator extends JoinIterator {
        NextOutcome rightOutcome;

        @Override
        protected int setupBuffer() {
            return setupBufferForIterator(left);
        }

        @Override
        public NextOutcome getNext() {
            final RecordPointer rightPointer = right.getRecordPointer();
            while (true) {
                if (curIdx == 0) {
                    rightOutcome = right.next();

                    if (rightOutcome == NextOutcome.NONE_LEFT) {
                        break;
                    }
                }

                final RecordBuffer bufferObj = buffer.get(curIdx++);
                Optional<JoinCondition> option = Iterables.tryFind(Lists.newArrayList(config.getConditions()), new Predicate<JoinCondition>() {
                    @Override
                    public boolean apply(JoinCondition condition) {
                        return eval(factory.getBasicEvaluator(rightPointer, condition.getRight()).eval(),
                                factory.getBasicEvaluator(bufferObj.pointer, condition.getLeft()).eval(), condition.getRelationship());
                    }
                });

                if (option.isPresent()) {
                    setJoinedRecord(rightPointer, bufferObj.pointer);
                    return (bufferObj.schemaChanged || rightOutcome == NextOutcome.INCREMENTED_SCHEMA_CHANGED) ?
                            NextOutcome.INCREMENTED_SCHEMA_CHANGED :
                            NextOutcome.INCREMENTED_SCHEMA_UNCHANGED;
                }

                if (curIdx >= bufferLength) {
                    curIdx = 0;
                }
            }

            return NextOutcome.NONE_LEFT;
        }
    }

    class RightIterator extends JoinIterator {
        NextOutcome rightOutcome;

        @Override
        protected int setupBuffer() {
            return setupBufferForIterator(left);
        }

        @Override
        public NextOutcome getNext() {
            final RecordPointer rightPointer = right.getRecordPointer();
            boolean isFound = true;
            while (true) {
                if (curIdx == 0) {
                    if (!isFound) {
                        setJoinedRecord(null, rightPointer);
                        return rightOutcome;
                    }

                    rightOutcome = right.next();

                    if (rightOutcome == NextOutcome.NONE_LEFT) {
                        break;
                    }

                    isFound = false;
                }

                final RecordBuffer bufferObj = buffer.get(curIdx++);
                Optional<JoinCondition> option = Iterables.tryFind(Lists.newArrayList(config.getConditions()), new Predicate<JoinCondition>() {
                    @Override
                    public boolean apply(JoinCondition condition) {
                        return eval(factory.getBasicEvaluator(rightPointer, condition.getRight()).eval(),
                                factory.getBasicEvaluator(bufferObj.pointer, condition.getLeft()).eval(), condition.getRelationship());
                    }
                });

                if (option.isPresent()) {
                    bufferObj.setHasJoined(true);
                    setJoinedRecord(rightPointer, bufferObj.pointer);
                    return (bufferObj.schemaChanged || rightOutcome == NextOutcome.INCREMENTED_SCHEMA_CHANGED) ?
                            NextOutcome.INCREMENTED_SCHEMA_CHANGED :
                            NextOutcome.INCREMENTED_SCHEMA_UNCHANGED;
                }

                if (curIdx >= bufferLength) {
                    curIdx = 0;
                }
            }

            return NextOutcome.NONE_LEFT;
        }
    }

    class OuterIterator extends RightIterator {
        boolean innerJoinCompleted = false;

        @Override
        public NextOutcome getNext() {
            if (innerJoinCompleted && curIdx >= bufferLength) {
                return NextOutcome.NONE_LEFT;
            }

            if (!innerJoinCompleted) {
                NextOutcome outcome = super.getNext();
                if (outcome != NextOutcome.NONE_LEFT) {
                    return outcome;
                } else {
                    innerJoinCompleted = true;
                    curIdx = 0;
                }
            }

            if (innerJoinCompleted) {
                while (curIdx < bufferLength) {
                    RecordBuffer recordBuffer = buffer.get(curIdx++);
                    if (!recordBuffer.hasJoined) {
                        setJoinedRecord(null, recordBuffer.pointer);
                        return recordBuffer.schemaChanged ? NextOutcome.INCREMENTED_SCHEMA_CHANGED : NextOutcome.INCREMENTED_SCHEMA_UNCHANGED;
                    }
                }
            }
            return NextOutcome.NONE_LEFT;
        }
    }
}
