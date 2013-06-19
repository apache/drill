package org.apache.drill.exec.store;

import com.carrotsearch.hppc.IntObjectOpenHashMap;
import com.carrotsearch.hppc.cursors.IntObjectCursor;
import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.google.common.base.Charsets;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.google.common.io.InputSupplier;
import com.google.common.io.Resources;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.proto.SchemaDefProtos;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.vector.*;
import org.apache.drill.exec.schema.*;
import org.apache.drill.exec.schema.json.jackson.JacksonHelper;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.List;

import static com.fasterxml.jackson.core.JsonToken.*;

public class JSONRecordReader implements RecordReader {
    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(JSONRecordReader.class);
    private static final int DEFAULT_LENGTH = 256 * 1024; // 256kb
    public static final Charset UTF_8 = Charset.forName("UTF-8");

    private final String inputPath;

    private final IntObjectOpenHashMap<VectorHolder> valueVectorMap;

    private JsonParser parser;
    private SchemaIdGenerator generator;
    private DiffSchema diffSchema;
    private RecordSchema currentSchema;
    private List<Field> removedFields;
    private OutputMutator outputMutator;
    private BufferAllocator allocator;
    private int batchSize;

    public JSONRecordReader(FragmentContext fragmentContext, String inputPath, int batchSize) {
        this.inputPath = inputPath;
        this.allocator = fragmentContext.getAllocator();
        this.batchSize = batchSize;
        valueVectorMap = new IntObjectOpenHashMap<>();
    }

    public JSONRecordReader(FragmentContext fragmentContext, String inputPath) {
        this(fragmentContext, inputPath, DEFAULT_LENGTH);
    }

    private JsonParser getParser() {
        return parser;
    }

    @Override
    public void setup(OutputMutator output) throws ExecutionSetupException {
        outputMutator = output;
        currentSchema = new ObjectSchema();
        diffSchema = new DiffSchema();
        removedFields = Lists.newArrayList();

        try {
            InputSupplier<InputStreamReader> input;
            if (inputPath.startsWith("resource:")) {
                input = Resources.newReaderSupplier(Resources.getResource(inputPath.substring(9)), Charsets.UTF_8);
            } else {
                input = Files.newReaderSupplier(new File(inputPath), Charsets.UTF_8);
            }

            JsonFactory factory = new JsonFactory();
            parser = factory.createJsonParser(input.getInput());
            parser.nextToken(); // Read to the first START_OBJECT token
            generator = new SchemaIdGenerator();
        } catch (IOException e) {
            throw new ExecutionSetupException(e);
        }
    }

    @Override
    public int next() {
        if (parser.isClosed() || !parser.hasCurrentToken()) {
            return 0;
        }

        resetBatch();

        int nextRowIndex = 0;

        try {
            while (ReadType.OBJECT.readRecord(null, this, null, nextRowIndex++)) {
                parser.nextToken(); // Read to START_OBJECT token

                if (!parser.hasCurrentToken()) {
                    parser.close();
                    break;
                }
            }

            parser.nextToken();

            if (!parser.hasCurrentToken()) {
                parser.close();
            }

            // Garbage collect fields never referenced in this batch
            for (Field field : Iterables.concat(currentSchema.removeUnreadFields(), removedFields)) {
                diffSchema.addRemovedField(field);
                outputMutator.removeField(field.getFieldId());
            }

        } catch (IOException | SchemaChangeException e) {
            logger.error("Error reading next in Json reader", e);
        }
        return nextRowIndex;
    }

    private void resetBatch() {
        for (ObjectCursor<VectorHolder> holder : valueVectorMap.values()) {
            holder.value.reset();
        }

        currentSchema.resetMarkedFields();
        diffSchema.reset();
        removedFields.clear();
    }

    @Override
    public void cleanup() {
        try {
            parser.close();
        } catch (IOException e) {
            logger.warn("Error closing Json parser", e);
        }
    }

    private SchemaIdGenerator getGenerator() {
        return generator;
    }

    private RecordSchema getCurrentSchema() {
        return currentSchema;
    }

    private void setCurrentSchema(RecordSchema schema) {
        currentSchema = schema;
    }

    private List<Field> getRemovedFields() {
        return removedFields;
    }

    private DiffSchema getDiffSchema() {
        return diffSchema;
    }

    public BufferAllocator getAllocator() {
        return allocator;
    }

    public OutputMutator getOutputMutator() {
        return outputMutator;
    }

    public static enum ReadType {
        ARRAY(END_ARRAY) {
            @Override
            public Field createField(RecordSchema parentSchema, int parentFieldId, IdGenerator<Integer> generator, String prefixFieldName, String fieldName, SchemaDefProtos.MajorType fieldType, int index) {
                return new OrderedField(parentSchema, parentFieldId, generator, fieldType, prefixFieldName, index);
            }

            @Override
            public RecordSchema createSchema() throws IOException {
                return new ListSchema();
            }
        },
        OBJECT(END_OBJECT) {
            @Override
            public Field createField(RecordSchema parentSchema,
                                     int parentFieldId,
                                     IdGenerator<Integer> generator,
                                     String prefixFieldName,
                                     String fieldName,
                                     SchemaDefProtos.MajorType fieldType,
                                     int index) {
                return new NamedField(parentSchema, parentFieldId, generator, prefixFieldName, fieldName, fieldType);
            }

            @Override
            public RecordSchema createSchema() throws IOException {
                return new ObjectSchema();
            }
        };

        private final JsonToken endObject;

        ReadType(JsonToken endObject) {
            this.endObject = endObject;
        }

        public JsonToken getEndObject() {
            return endObject;
        }

        public boolean readRecord(Field parentField,
                                  JSONRecordReader reader,
                                  String prefixFieldName,
                                  int rowIndex) throws IOException, SchemaChangeException {
            JsonParser parser = reader.getParser();
            JsonToken token = parser.nextToken();
            JsonToken endObject = getEndObject();
            int colIndex = 0;
            boolean isFull = false;
            while (token != endObject) {
                if (token == FIELD_NAME) {
                    token = parser.nextToken();
                    continue;
                }

                String fieldName = parser.getCurrentName();
                SchemaDefProtos.MajorType fieldType = JacksonHelper.getFieldType(token);
                ReadType readType = null;
                switch (token) {
                    case START_ARRAY:
                        readType = ReadType.ARRAY;
                        break;
                    case START_OBJECT:
                        readType = ReadType.OBJECT;
                        break;
                }
                if (fieldType != null) { // Including nulls
                    isFull = isFull ||
                            !recordData(
                                    parentField,
                                    readType,
                                    reader,
                                    fieldType,
                                    prefixFieldName,
                                    fieldName,
                                    rowIndex, colIndex);
                }
                token = parser.nextToken();
                colIndex += 1;
            }
            return !isFull;
        }

        private void removeChildFields(List<Field> removedFields, Field field) {
            RecordSchema schema = field.getAssignedSchema();
            if (schema == null) {
                return;
            }
            for (Field childField : schema.getFields()) {
                removedFields.add(childField);
                if (childField.hasSchema()) {
                    removeChildFields(removedFields, childField);
                }
            }
        }

        private boolean recordData(Field parentField,
                                   JSONRecordReader.ReadType readType,
                                   JSONRecordReader reader,
                                   SchemaDefProtos.MajorType fieldType,
                                   String prefixFieldName,
                                   String fieldName,
                                   int rowIndex,
                                   int colIndex) throws IOException, SchemaChangeException {
            RecordSchema currentSchema = reader.getCurrentSchema();
            Field field = currentSchema.getField(fieldName, colIndex);
            boolean isFieldFound = field != null;
            List<Field> removedFields = reader.getRemovedFields();
            int parentFieldId = parentField == null ? 0 : parentField.getFieldId();
            if (!isFieldFound || !field.getFieldType().equals(fieldType)) {
                if (isFieldFound) {
                    if (field.hasSchema()) {
                        removeChildFields(removedFields, field);
                    }
                    removedFields.add(field);
                    currentSchema.removeField(field, colIndex);
                }

                field = createField(
                        currentSchema,
                        parentFieldId,
                        reader.getGenerator(),
                        prefixFieldName,
                        fieldName,
                        fieldType,
                        colIndex
                );

                reader.recordNewField(field);
                currentSchema.addField(field);
            }

            field.setRead(true);

            VectorHolder holder = getOrCreateVectorHolder(reader, field, parentFieldId);
            if (readType != null) {
                RecordSchema fieldSchema = field.getAssignedSchema();
                reader.setCurrentSchema(fieldSchema);

                RecordSchema newSchema = readType.createSchema();
                field.assignSchemaIfNull(newSchema);

                if (fieldSchema == null) reader.setCurrentSchema(newSchema);
                readType.readRecord(field, reader, field.getFullFieldName(), rowIndex);

                reader.setCurrentSchema(currentSchema);
            } else {
                return addValueToVector(
                        rowIndex,
                        holder,
                        reader.getAllocator(),
                        JacksonHelper.getValueFromFieldType(
                                reader.getParser(),
                                fieldType.getMinorType()
                        ),
                        fieldType.getMinorType()
                );
            }

            return true;
        }

        private static <T> boolean addValueToVector(int index, VectorHolder holder, BufferAllocator allocator, T val, SchemaDefProtos.MinorType minorType) {
            switch (minorType) {
                case INT: {
                    holder.incAndCheckLength(32);
                    ValueVector.NullableInt int4 = (ValueVector.NullableInt) holder.getValueVector();
                    if (val == null) {
                      int4.setNull(index);
                    } else {
                      int4.set(index, (Integer) val);
                    }
                    return holder.hasEnoughSpace(32);
                }
                case FLOAT4: {
                    holder.incAndCheckLength(32);
                    ValueVector.NullableFloat4 float4 = (ValueVector.NullableFloat4) holder.getValueVector();
                    if (val == null) {
                      float4.setNull(index);
                    } else {
                      float4.set(index, (Float) val);
                    }
                    return holder.hasEnoughSpace(32);
                }
                case VARCHAR4: {
                    if (val == null) {
                        ((ValueVector.NullableVarChar4) holder.getValueVector()).setNull(index);
                        return (index + 1) * 4 <= holder.getLength();
                    } else {
                        byte[] bytes = ((String) val).getBytes(UTF_8);
                        int length = bytes.length;
                        holder.incAndCheckLength(length);
                        ValueVector.NullableVarChar4 varLen4 = (ValueVector.NullableVarChar4) holder.getValueVector();
                        varLen4.set(index, bytes);
                        return holder.hasEnoughSpace(length);
                    }
                }
                case BOOLEAN: {
                    holder.incAndCheckLength(1);
                    ValueVector.NullableBit bit = (ValueVector.NullableBit) holder.getValueVector();
                    if (val != null) {
                        bit.set(index, (Boolean)val ? 1 : 0);
                    }
                    return holder.hasEnoughSpace(1);
                }
                default:
                    throw new DrillRuntimeException("Type not supported to add value. Type: " + minorType);
            }
        }

        private VectorHolder getOrCreateVectorHolder(JSONRecordReader reader, Field field, int parentFieldId) throws SchemaChangeException {
            return reader.getOrCreateVectorHolder(field, parentFieldId);
        }

        public abstract RecordSchema createSchema() throws IOException;

        public abstract Field createField(RecordSchema parentSchema,
                                          int parentFieldId,
                                          IdGenerator<Integer> generator,
                                          String prefixFieldName,
                                          String fieldName,
                                          SchemaDefProtos.MajorType fieldType,
                                          int index);
    }

    private void recordNewField(Field field) {
        diffSchema.recordNewField(field);
    }

    private VectorHolder getOrCreateVectorHolder(Field field, int parentFieldId) throws SchemaChangeException {
        if (!valueVectorMap.containsKey(field.getFieldId())) {
            SchemaDefProtos.MajorType type = field.getFieldType();
            int fieldId = field.getFieldId();
            MaterializedField f = MaterializedField.create(new SchemaPath(field.getFieldName()), fieldId, parentFieldId, type);
            ValueVector.Base v = TypeHelper.getNewVector(f, allocator);
            v.allocateNew(batchSize);
            VectorHolder holder = new VectorHolder(batchSize, v);
            valueVectorMap.put(fieldId, holder);
            outputMutator.addField(fieldId, v);
            return holder;
        }
        return valueVectorMap.lget();
    }
}
