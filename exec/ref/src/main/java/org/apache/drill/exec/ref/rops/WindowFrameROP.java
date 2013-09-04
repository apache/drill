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

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Queues;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.logical.data.WindowFrame;
import org.apache.drill.exec.ref.RecordIterator;
import org.apache.drill.exec.ref.RecordPointer;
import org.apache.drill.exec.ref.values.DataValue;
import org.apache.drill.exec.ref.values.ScalarValues;

import java.util.List;
import java.util.Queue;

import static com.google.common.base.Preconditions.checkArgument;

public class WindowFrameROP extends SingleInputROPBase<WindowFrame> {

    private RecordIterator incoming;
    private FieldReference segmentRef;
    private FieldReference positionRef;
    private FieldReference withinRef;
    private boolean withinConstrained;

    public WindowFrameROP(WindowFrame config) {
        super(config);
        WindowFrame.FrameRef frameRef = config.getFrame();
        if (frameRef != null) {
            positionRef = frameRef.getPosition();
            segmentRef = frameRef.getSegment();
        }

        if (positionRef == null) {
            positionRef = new FieldReference("ref.position", ExpressionPosition.UNKNOWN);
        }

        if (segmentRef == null) {
            segmentRef = new FieldReference("ref.segment", ExpressionPosition.UNKNOWN);
        }

        withinRef = config.getWithin();
        withinConstrained = withinRef != null;
    }

    @Override
    protected void setInput(RecordIterator incoming) {
        this.incoming = incoming;
    }

    @Override
    protected RecordIterator getIteratorInternal() {
        return new WindowIterator(config.getStart(), config.getEnd());
    }

    private class Window {
        List<WindowObjectHolder> holders;
        int windowId;
        int nextRecordIndex;
        int lastIndex;
        int nextPosition;

        private Window(long start, long end, Window lastWindow) {
            this.holders = Lists.newArrayList();
            if (lastWindow != null) {
                this.windowId = lastWindow.getWindowId() + 1;
                lastIndex = (int) (windowId + end);
                final int lastMinIndex = (int) Math.max(windowId + start, 0);
                Iterable<WindowObjectHolder> lastHolders = Iterables.filter(lastWindow.getHolders(), new Predicate<WindowObjectHolder>() {
                    @Override
                    public boolean apply(WindowObjectHolder windowObjectHolder) {
                        return windowObjectHolder.getIndex() >= lastMinIndex;
                    }
                });
                for (WindowObjectHolder holder : lastHolders) {
                    holder.setPosition(nextPosition());
                    addRecord(holder);
                }
            } else {
                this.windowId = 0;
                lastIndex = (int) (windowId + end);
            }

        }

        private int getWindowId() {
            return windowId;
        }

        private boolean isFull() {
            if (holders.isEmpty()) {
                return false;
            }

            WindowObjectHolder lastHolder = holders.get(holders.size() - 1);
            return lastHolder.getIndex() >= lastIndex;
        }

        private void addRecord(RecordPointer pointer, int index, boolean schemaChanged) {
            addRecord(new WindowObjectHolder(pointer, nextPosition(), index, schemaChanged));
        }

        private void addRecord(WindowObjectHolder holder) {
            if (!isFull()) {
                holders.add(holder);
            } else {
                throw new DrillRuntimeException("Adding more records into windows then configured.");
            }
        }

        private int nextPosition() {
            int position = nextPosition;
            if (nextPosition == 0) {
                nextPosition = 1;
            } else if (nextPosition > 0) {
                nextPosition = -nextPosition;
            } else {
                nextPosition = -nextPosition + 1;
            }
            return position;
        }

        public List<WindowObjectHolder> getHolders() {
            return holders;
        }

        public WindowObjectHolder nextRecord() {
            if (nextRecordIndex >= holders.size()) {
                return null;
            } else {
                return holders.get(nextRecordIndex++);
            }
        }

        public boolean isCrossedWithinBoundary(RecordPointer nextRecord) {
            if (withinConstrained && nextRecord != null && !holders.isEmpty()) {
                DataValue lastWithinVal = holders.get(holders.size() - 1).getPointer().getField(withinRef);
                DataValue nextWithinVal = nextRecord.getField(withinRef);
                boolean lastIsNull = lastWithinVal == null;
                boolean nextIsNull = nextWithinVal == null;
                return lastIsNull != nextIsNull || (!nextIsNull && !nextWithinVal.equals(lastWithinVal));
            }

            return false;
        }

        public void removeHoldersBeforeIndex(final int index) {
            Iterables.removeIf(holders, new Predicate<WindowObjectHolder>() {
                @Override
                public boolean apply(WindowObjectHolder windowObjectHolder) {
                    return windowObjectHolder.getIndex() <= index;
                }
            });
        }
    }

    private class WindowObjectHolder {
        private final RecordPointer pointer;
        private int position;
        private final int index;
        private final boolean schemaChanged;
        private int windowId;

        private WindowObjectHolder(RecordPointer pointer, int position, int index, boolean schemaChanged) {
            this.pointer = pointer;
            this.position = position;
            this.index = index;
            this.schemaChanged = schemaChanged;
        }

        public WindowObjectHolder setWindowId(int windowId) {
            this.windowId = windowId;
            return this;
        }

        public RecordPointer getPointer() {
            return pointer;
        }

        public int getPosition() {
            return position;
        }

        public int getIndex() {
            return index;
        }

        public boolean isSchemaChanged() {
            return schemaChanged;
        }

        public int getWindowId() {
            return windowId;
        }

        public void setPosition(int position) {
            this.position = position;
        }
    }

    private class WindowManager {
        Queue<WindowObjectHolder> holderBuffer;
        Window curWindow;
        long start;
        long end;

        WindowManager(long start, long end) {
            holderBuffer = Queues.newLinkedBlockingDeque();
            this.start = start;
            this.end = end;
        }

        private WindowObjectHolder nextRecord() {
            if (curWindow != null) {
                WindowObjectHolder holder = curWindow.nextRecord();
                if (holder != null) {
                    return holder.setWindowId(curWindow.getWindowId());
                } else if (!holderBuffer.isEmpty()) {
                    WindowObjectHolder obj = holderBuffer.poll();
                    addRecord(obj.getPointer(), obj.getIndex(), obj.isSchemaChanged());
                    return nextRecord();
                }
            }

            return null;
        }

        public void addRecord(RecordPointer recordPointer, int index, boolean schemaChanged) {
            if (curWindow == null || curWindow.isFull()) {
                curWindow = new Window(start, end, curWindow);
            } else if (curWindow.isCrossedWithinBoundary(recordPointer)) {
                if (index - 1 == curWindow.getWindowId()) {
                    curWindow.removeHoldersBeforeIndex(curWindow.getWindowId());
                }
                curWindow = new Window(start, end, curWindow);
            }

            if (curWindow.isCrossedWithinBoundary(recordPointer)) {
                holderBuffer.add(new WindowObjectHolder(recordPointer, 0, index, schemaChanged));
            } else {
                curWindow.addRecord(recordPointer, index, schemaChanged);
            }
        }

        public boolean hasMoreWindows(int curIndex) {
            return curWindow != null && curWindow.getWindowId() < curIndex;
        }

        public WindowObjectHolder nextWindowOnEmpty() {
            curWindow = new Window(start, end, curWindow);
            return nextRecord();
        }
    }


    private class WindowIterator implements RecordIterator {
        private int curIndex = -1;
        private WindowManager windowManager;
        private ProxySimpleRecord windowPointer;

        public WindowIterator(long start, long end) {
            checkArgument(end - start >= 0, "Invalid end and start. end: %s, start: %s", end, start);
            windowManager = new WindowManager(start, end);
            windowPointer = new ProxySimpleRecord();
        }

        @Override
        public RecordPointer getRecordPointer() {
            return windowPointer;
        }

        @Override
        public NextOutcome next() {
            WindowObjectHolder holder = windowManager.nextRecord();
            if (holder == null) {
                NextOutcome outcome = incoming.next();
                if (outcome != NextOutcome.NONE_LEFT) {
                    windowManager.addRecord(incoming.getRecordPointer().copy(), ++curIndex, outcome == NextOutcome.INCREMENTED_SCHEMA_CHANGED);
                    holder = windowManager.nextRecord();
                } else if (windowManager.hasMoreWindows(curIndex)) {
                    holder = windowManager.nextWindowOnEmpty();
                }

                if (holder == null) {
                    return NextOutcome.NONE_LEFT;
                }
            }

            RecordPointer newPointer = holder.getPointer().copy();
            newPointer.addField(positionRef, new ScalarValues.IntegerScalar(holder.getPosition()));
            newPointer.addField(segmentRef, new ScalarValues.IntegerScalar(holder.getWindowId()));
            windowPointer.setRecord(newPointer);
            return holder.isSchemaChanged() ? NextOutcome.INCREMENTED_SCHEMA_CHANGED : NextOutcome.INCREMENTED_SCHEMA_UNCHANGED;
        }

        @Override
        public ROP getParent() {
            return WindowFrameROP.this;
        }
    }
}
