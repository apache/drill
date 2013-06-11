package org.apache.drill.exec.store;

import com.beust.jcommander.internal.Lists;
import mockit.Expectations;
import mockit.Injectable;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.memory.DirectBufferAllocator;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.proto.SchemaDefProtos;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.record.vector.ValueVector;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class JSONRecordReaderTest {
    private static final Charset UTF_8 = Charset.forName("UTF-8");

    private String getResource(String resourceName) {
        return "src/test/resources/" + resourceName;
    }

    class MockOutputMutator implements OutputMutator {
        List<Integer> removedFields = Lists.newArrayList();
        List<ValueVector> addFields = Lists.newArrayList();

        @Override
        public void removeField(int fieldId) throws SchemaChangeException {
            removedFields.add(fieldId);
        }

        @Override
        public void addField(int fieldId, ValueVector<?> vector) throws SchemaChangeException {
            addFields.add(vector);
        }

        @Override
        public void setNewSchema() throws SchemaChangeException {
        }

        List<Integer> getRemovedFields() {
            return removedFields;
        }

        List<ValueVector> getAddFields() {
            return addFields;
        }
    }

    private <T> void assertField(ValueVector valueVector, int index, SchemaDefProtos.MinorType expectedMinorType, T value, String name) {
        UserBitShared.FieldMetadata metadata = valueVector.getMetadata();
        SchemaDefProtos.FieldDef def = metadata.getDef();
        assertEquals(expectedMinorType, def.getMajorType().getMinorType());
        T val = (T) valueVector.getObject(index);
        if(val instanceof byte[]) {
            assertTrue(Arrays.equals((byte[]) value, (byte[]) val));
        } else {
            assertEquals(value, val);
        }
        assertEquals(name, def.getNameList().get(0).getName());
    }

    @Test
    public void testSameSchemaInSameBatch(@Injectable final FragmentContext context) throws IOException, ExecutionSetupException {
        new Expectations() {
            {
                context.getAllocator();
                returns(new DirectBufferAllocator());
            }
        };

        JSONRecordReader jr = new JSONRecordReader(context, getResource("scan_json_test_1.json"));

        MockOutputMutator mutator = new MockOutputMutator();
        List<ValueVector> addFields = mutator.getAddFields();
        jr.setup(mutator);
        assertEquals(2, jr.next());
        assertEquals(3, addFields.size());
        assertField(addFields.get(0), 0, SchemaDefProtos.MinorType.INT, 123, "test");
        assertField(addFields.get(1), 0, SchemaDefProtos.MinorType.BOOLEAN, 1, "b");
        assertField(addFields.get(2), 0, SchemaDefProtos.MinorType.VARCHAR4, "hi!".getBytes(UTF_8), "c");
        assertField(addFields.get(0), 1, SchemaDefProtos.MinorType.INT, 1234, "test");
        assertField(addFields.get(1), 1, SchemaDefProtos.MinorType.BOOLEAN, 0, "b");
        assertField(addFields.get(2), 1, SchemaDefProtos.MinorType.VARCHAR4, "drill!".getBytes(UTF_8), "c");

        assertEquals(0, jr.next());
        assertTrue(mutator.getRemovedFields().isEmpty());
    }

    @Test
    public void testChangedSchemaInSameBatch(@Injectable final FragmentContext context) throws IOException, ExecutionSetupException {
        new Expectations() {
            {
                context.getAllocator();
                returns(new DirectBufferAllocator());
            }
        };

        JSONRecordReader jr = new JSONRecordReader(context, getResource("scan_json_test_2.json"));
        MockOutputMutator mutator = new MockOutputMutator();
        List<ValueVector> addFields = mutator.getAddFields();

        jr.setup(mutator);
        assertEquals(3, jr.next());
        assertEquals(6, addFields.size());
        assertField(addFields.get(0), 0, SchemaDefProtos.MinorType.INT, 123, "test");
        assertField(addFields.get(1), 0, SchemaDefProtos.MinorType.INT, 1, "b");
        assertField(addFields.get(2), 0, SchemaDefProtos.MinorType.INT, 2, "c");
        assertField(addFields.get(3), 0, SchemaDefProtos.MinorType.VARCHAR4, "test1".getBytes(UTF_8), "str1");
        assertField(addFields.get(0), 1, SchemaDefProtos.MinorType.INT, 1234, "test");
        assertField(addFields.get(1), 1, SchemaDefProtos.MinorType.INT, 3, "b");
        assertField(addFields.get(3), 1, SchemaDefProtos.MinorType.VARCHAR4, "test2".getBytes(UTF_8), "str1");
        assertField(addFields.get(4), 1, SchemaDefProtos.MinorType.INT, 4, "d");
        assertField(addFields.get(0), 2, SchemaDefProtos.MinorType.INT, 12345, "test");
        assertField(addFields.get(2), 2, SchemaDefProtos.MinorType.INT, 5, "c");
        assertField(addFields.get(4), 2, SchemaDefProtos.MinorType.INT, 6, "d");
        assertField(addFields.get(5), 2, SchemaDefProtos.MinorType.VARCHAR4, "test3".getBytes(UTF_8), "str2");
        assertTrue(mutator.getRemovedFields().isEmpty());
        assertEquals(0, jr.next());
    }

    @Test
    public void testChangedSchemaInTwoBatches(@Injectable final FragmentContext context) throws IOException, ExecutionSetupException {
        new Expectations() {
            {
                context.getAllocator();
                returns(new DirectBufferAllocator());
            }
        };

        JSONRecordReader jr = new JSONRecordReader(context, getResource("scan_json_test_2.json"), 64); // batch only fits 1 int
        MockOutputMutator mutator = new MockOutputMutator();
        List<ValueVector> addFields = mutator.getAddFields();
        List<Integer> removedFields = mutator.getRemovedFields();

        jr.setup(mutator);
        assertEquals(1, jr.next());
        assertEquals(4, addFields.size());
        assertField(addFields.get(0), 0, SchemaDefProtos.MinorType.INT, 123, "test");
        assertField(addFields.get(1), 0, SchemaDefProtos.MinorType.INT, 1, "b");
        assertField(addFields.get(2), 0, SchemaDefProtos.MinorType.INT, 2, "c");
        assertField(addFields.get(3), 0, SchemaDefProtos.MinorType.VARCHAR4, "test1".getBytes(UTF_8), "str1");
        addFields.clear();
        assertEquals(1, jr.next());
        assertEquals(4, addFields.size());
        assertField(addFields.get(0), 0, SchemaDefProtos.MinorType.INT, 1234, "test");
        assertField(addFields.get(1), 0, SchemaDefProtos.MinorType.INT, 3, "b");
        assertField(addFields.get(2), 0, SchemaDefProtos.MinorType.INT, 4, "d");
        assertField(addFields.get(3), 0, SchemaDefProtos.MinorType.VARCHAR4, "test2".getBytes(UTF_8), "str1");
        assertEquals(1, removedFields.size());
        assertEquals(3, (int) removedFields.get(0));
        removedFields.clear();
        addFields.clear();
        assertEquals(1, jr.next());
        assertEquals(4, addFields.size());
        assertField(addFields.get(0), 0, SchemaDefProtos.MinorType.INT, 12345, "test");
        assertField(addFields.get(1), 0, SchemaDefProtos.MinorType.INT, 5, "c");
        assertField(addFields.get(2), 0, SchemaDefProtos.MinorType.INT, 6, "d");
        assertField(addFields.get(3), 0, SchemaDefProtos.MinorType.VARCHAR4, "test3".getBytes(UTF_8), "str2");
        assertEquals(2, removedFields.size());
        assertTrue(removedFields.contains(4));
        assertTrue(removedFields.contains(2));
        assertEquals(0, jr.next());
    }

    /*

    @Test
    public void testScanJsonRemovedOneField() throws IOException {
        ScanJson sj = new ScanJson(getResource("scan_json_test_3.json"));
        PhysicalOperatorIterator iterator = sj.getIterator();
        expectSchemaChanged(iterator);
        DiffSchema diffSchema = expectSchemaChanged(iterator).getSchemaChanges();
        assertEquals(0, diffSchema.getAddedFields().size());
        assertEquals(1, diffSchema.getRemovedFields().size());
        assertEquals(PhysicalOperatorIterator.NextOutcome.NONE_LEFT, iterator.next());
    }

    @Test
    public void testScanJsonAddOneRemoveOne() throws IOException {
        ScanJson sj = new ScanJson(getResource("scan_json_test_4.json"));
        PhysicalOperatorIterator iterator = sj.getIterator();
        expectSchemaChanged(iterator);
        DiffSchema diffSchema = expectSchemaChanged(iterator).getSchemaChanges();
        assertEquals(1, diffSchema.getAddedFields().size());
        assertEquals(1, diffSchema.getRemovedFields().size());
        assertEquals(PhysicalOperatorIterator.NextOutcome.NONE_LEFT, iterator.next());
    }

    @Test
    public void testScanJsonCycleAdditions() throws IOException {
        ScanJson sj = new ScanJson(getResource("scan_json_test_5.json"));
        PhysicalOperatorIterator iterator = sj.getIterator();
        expectSchemaChanged(iterator);
        DiffSchema diffSchema = expectSchemaChanged(iterator).getSchemaChanges();
        assertEquals(1, diffSchema.getAddedFields().size());
        assertEquals(1, diffSchema.getRemovedFields().size());
        diffSchema = expectSchemaChanged(iterator).getSchemaChanges();
        assertEquals(1, diffSchema.getAddedFields().size());
        assertEquals(Field.FieldType.FLOAT, diffSchema.getAddedFields().get(0).getFieldType());
        assertEquals("test2", diffSchema.getAddedFields().get(0).getFieldName());
        assertEquals(1, diffSchema.getRemovedFields().size());
        assertEquals(Field.FieldType.BOOLEAN, diffSchema.getRemovedFields().get(0).getFieldType());
        assertEquals("test3", diffSchema.getRemovedFields().get(0).getFieldName());
        assertEquals(PhysicalOperatorIterator.NextOutcome.NONE_LEFT, iterator.next());
    }

    @Test
    public void testScanJsonModifiedOneFieldType() throws IOException {
        ScanJson sj = new ScanJson(getResource("scan_json_test_6.json"));
        PhysicalOperatorIterator iterator = sj.getIterator();
        expectSchemaChanged(iterator);
        DiffSchema diffSchema = expectSchemaChanged(iterator).getSchemaChanges();
        List<Field> addedFields = diffSchema.getAddedFields();
        assertEquals(4, addedFields.size());
        List<Field> removedFields = diffSchema.getRemovedFields();
        assertEquals(4, removedFields.size());
        assertFieldExists("test", Field.FieldType.STRING, addedFields);
        assertFieldExists("test2", Field.FieldType.BOOLEAN, addedFields);
        assertFieldExists("b", Field.FieldType.ARRAY, addedFields);
        assertFieldExists("[0]", Field.FieldType.INTEGER, addedFields);
        assertFieldExists("test", Field.FieldType.INTEGER, removedFields);
        assertFieldExists("test2", Field.FieldType.ARRAY, removedFields);
        assertFieldExists("b", Field.FieldType.INTEGER, removedFields);
        assertFieldExists("[0]", Field.FieldType.INTEGER, removedFields);
        assertEquals(PhysicalOperatorIterator.NextOutcome.NONE_LEFT, iterator.next());
    }

    private void expectSchemaChanged(PhysicalOperatorIterator iterator) throws IOException {
    }

    private void expectDataRecord(PhysicalOperatorIterator iterator) throws IOException {
    }
*/
}
