package org.apache.drill.exec.ref.rops;

import com.google.common.collect.Lists;

import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.data.WindowFrame;
import org.apache.drill.exec.ref.RecordIterator;
import org.apache.drill.exec.ref.RecordPointer;
import org.apache.drill.exec.ref.TestUtils;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static junit.framework.Assert.assertEquals;

public class WindowFrameROPTest {
    final String input = "" +
            "{id: 0}" +
            "{id: 1}" +
            "{id: 2}" +
            "{id: 3}" +
            "{id: 4}";

    @Test
    public void windowShouldWorkWithBefore() throws IOException {
        WindowFrameROP rop = new WindowFrameROP(new WindowFrame(null, null, -2L, 0L));
        RecordIterator incoming = TestUtils.jsonToRecordIterator("test", input);
        rop.setInput(incoming);
        RecordIterator out = rop.getOutput();

        List<WindowObj> windows = Lists.newArrayList(
                new WindowObj(0, 0, 0),
                new WindowObj(1, 0, 0),
                new WindowObj(1, 1, 1),
                new WindowObj(2, 0, 0),
                new WindowObj(2, 1, 1),
                new WindowObj(2, 2, -1),
                new WindowObj(3, 1, 0),
                new WindowObj(3, 2, 1),
                new WindowObj(3, 3, -1),
                new WindowObj(4, 2, 0),
                new WindowObj(4, 3, 1),
                new WindowObj(4, 4, -1)
        );

        verifyWindowOrder(windows, out);
    }

    @Test
    public void windowShouldWorkWithAfter() throws IOException {
        WindowFrameROP rop = new WindowFrameROP(new WindowFrame(null, null, 0L, 2L));
        RecordIterator incoming = TestUtils.jsonToRecordIterator("test", input);
        rop.setInput(incoming);
        RecordIterator out = rop.getOutput();

        List<WindowObj> windows = Lists.newArrayList(
                new WindowObj(0, 0, 0),
                new WindowObj(0, 1, 1),
                new WindowObj(0, 2, -1),
                new WindowObj(1, 1, 0),
                new WindowObj(1, 2, 1),
                new WindowObj(1, 3, -1),
                new WindowObj(2, 2, 0),
                new WindowObj(2, 3, 1),
                new WindowObj(2, 4, -1),
                new WindowObj(3, 3, 0),
                new WindowObj(3, 4, 1),
                new WindowObj(4, 4, 0)
        );

        verifyWindowOrder(windows, out);
    }

    @Test
    public void windowShouldWorkWithBeforeAndAfter() throws IOException {
        WindowFrameROP rop = new WindowFrameROP(new WindowFrame(null, null, -2L, 2L));
        RecordIterator incoming = TestUtils.jsonToRecordIterator("test", input);
        rop.setInput(incoming);
        RecordIterator out = rop.getOutput();

        List<WindowObj> windows = Lists.newArrayList(
                new WindowObj(0, 0, 0),
                new WindowObj(0, 1, 1),
                new WindowObj(0, 2, -1),
                new WindowObj(1, 0, 0),
                new WindowObj(1, 1, 1),
                new WindowObj(1, 2, -1),
                new WindowObj(1, 3, 2),
                new WindowObj(2, 0, 0),
                new WindowObj(2, 1, 1),
                new WindowObj(2, 2, -1),
                new WindowObj(2, 3, 2),
                new WindowObj(2, 4, -2),
                new WindowObj(3, 1, 0),
                new WindowObj(3, 2, 1),
                new WindowObj(3, 3, -1),
                new WindowObj(3, 4, 2),
                new WindowObj(4, 2, 0),
                new WindowObj(4, 3, 1),
                new WindowObj(4, 4, -1)
        );

        verifyWindowOrder(windows, out);
    }

    @Test
    public void windowShouldNotCrossWithin() throws IOException {
        String withinInput = "" +
                "{id: 0, v: 0}" +
                "{id: 1, v: 1}" +
                "{id: 2, v: 2}" +
                "{id: 3, v: 3}" +
                "{id: 4, v: 4}";
        WindowFrameROP rop = new WindowFrameROP(new WindowFrame(new FieldReference("test.v", ExpressionPosition.UNKNOWN), null, -2L, 2L));
        RecordIterator incoming = TestUtils.jsonToRecordIterator("test", withinInput);
        rop.setInput(incoming);
        RecordIterator out = rop.getOutput();

        List<WindowObj> windows = Lists.newArrayList(
                new WindowObj(0, 0, 0),
                new WindowObj(1, 1, 0),
                new WindowObj(2, 2, 0),
                new WindowObj(3, 3, 0),
                new WindowObj(4, 4, 0)
        );

        verifyWindowOrder(windows, out);
    }

    @Test
    public void windowShouldNotCrossWithins() throws IOException {
        String withinInput = "" +
                "{id: 0, v: 0}" +
                "{id: 1, v: 0}" +
                "{id: 2, v: 1}" +
                "{id: 3, v: 1}" +
                "{id: 4, v: 2}";
        WindowFrameROP rop = new WindowFrameROP(new WindowFrame(new FieldReference("test.v", ExpressionPosition.UNKNOWN), null, -1L, 2L));
        RecordIterator incoming = TestUtils.jsonToRecordIterator("test", withinInput);
        rop.setInput(incoming);
        RecordIterator out = rop.getOutput();

        List<WindowObj> windows = Lists.newArrayList(
                new WindowObj(0, 0, 0),
                new WindowObj(0, 1, 1),
                new WindowObj(1, 0, 0),
                new WindowObj(1, 1, 1),
                new WindowObj(2, 2, 0),
                new WindowObj(2, 3, 1),
                new WindowObj(3, 2, 0),
                new WindowObj(3, 3, 1),
                new WindowObj(4, 4, 0)
        );

        verifyWindowOrder(windows, out);
    }

    @Test
    public void windowShouldNotCrossWithinAndRange() throws IOException {
        String withinInput = "" +
                "{id: 0, v: 0}" +
                "{id: 1, v: 0}" +
                "{id: 2, v: 0}" +
                "{id: 3, v: 0}" +
                "{id: 4, v: 1}" +
                "{id: 5, v: 1}" +
                "{id: 6, v: 2}";
        WindowFrameROP rop = new WindowFrameROP(new WindowFrame(new FieldReference("test.v", ExpressionPosition.UNKNOWN), null, -1L, 3L));
        RecordIterator incoming = TestUtils.jsonToRecordIterator("test", withinInput);
        rop.setInput(incoming);
        RecordIterator out = rop.getOutput();

        List<WindowObj> windows = Lists.newArrayList(
                new WindowObj(0, 0, 0),
                new WindowObj(0, 1, 1),
                new WindowObj(0, 2, -1),
                new WindowObj(0, 3, 2),
                new WindowObj(1, 0, 0),
                new WindowObj(1, 1, 1),
                new WindowObj(1, 2, -1),
                new WindowObj(1, 3, 2),
                new WindowObj(2, 1, 0),
                new WindowObj(2, 2, 1),
                new WindowObj(2, 3, -1),
                new WindowObj(3, 2, 0),
                new WindowObj(3, 3, 1),
                new WindowObj(4, 4, 0),
                new WindowObj(4, 5, 1),
                new WindowObj(5, 4, 0),
                new WindowObj(5, 5, 1),
                new WindowObj(6, 6, 0)
        );

        verifyWindowOrder(windows, out);
    }

    private void verifyWindowOrder(List<WindowObj> expectedIds, RecordIterator out) {
        verifyWindowOrder(expectedIds, out, new SchemaPath("ref.segment", ExpressionPosition.UNKNOWN), new SchemaPath("ref.position", ExpressionPosition.UNKNOWN));
    }

    private void verifyWindowOrder(List<WindowObj> expectedIds, RecordIterator out, SchemaPath segment, SchemaPath position) {
        RecordIterator.NextOutcome outcome = out.next();
        RecordPointer pointer = out.getRecordPointer();
        int count = 0;
        SchemaPath id = new SchemaPath("test.id", ExpressionPosition.UNKNOWN);
        int expectedSize = expectedIds.size();
        while (outcome != RecordIterator.NextOutcome.NONE_LEFT) {
            count += 1;
            WindowObj windowObj = expectedIds.get(count - 1);
            //System.out.println(windowObj);
            assertEquals("Id mismatch", windowObj.id, pointer.getField(id).getAsNumeric().getAsInt());
            assertEquals("Window id mismatch", windowObj.windowId, pointer.getField(segment).getAsNumeric().getAsInt());
            assertEquals("Position mismatch", windowObj.position, pointer.getField(position).getAsNumeric().getAsInt());
            outcome = out.next();
        }
        assertEquals(expectedSize, count);
    }

    private class WindowObj {
        int id;
        int position;
        int windowId;

        private WindowObj(int windowId, int id, int position) {
            this.id = id;
            this.position = position;
            this.windowId = windowId;
        }

        @Override
        public String toString() {
            return "WindowObj{" +
                    "id=" + id +
                    ", position=" + position +
                    ", windowId=" + windowId +
                    '}';
        }
    }
}
