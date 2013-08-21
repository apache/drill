package org.apache.drill.exec.store.json;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.google.common.base.Charsets;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import com.google.common.io.InputSupplier;
import com.google.common.io.Resources;

import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.schema.*;
import org.apache.drill.exec.schema.json.jackson.JacksonHelper;
import org.apache.drill.exec.store.RecordReader;
import org.apache.drill.exec.store.VectorHolder;
import org.apache.drill.exec.vector.*;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;

import static com.fasterxml.jackson.core.JsonToken.*;

public class JSONRecordReader implements RecordReader {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(JSONRecordReader.class);
  private static final int DEFAULT_LENGTH = 256 * 1024; // 256kb
  public static final Charset UTF_8 = Charset.forName("UTF-8");

  private final Map<String, VectorHolder> valueVectorMap;
  private final FileSystem fileSystem;
  private final Path hadoopPath;

  private JsonParser parser;
  private SchemaIdGenerator generator;
  private DiffSchema diffSchema;
  private RecordSchema currentSchema;
  private List<Field> removedFields;
  private OutputMutator outputMutator;
  private BufferAllocator allocator;
  private int batchSize;

  public JSONRecordReader(FragmentContext fragmentContext, String inputPath, FileSystem fileSystem, int batchSize) {
    this.hadoopPath = new Path(inputPath);
    this.fileSystem = fileSystem;
    this.allocator = fragmentContext.getAllocator();
    this.batchSize = batchSize;
    valueVectorMap = Maps.newHashMap();
  }

  public JSONRecordReader(FragmentContext fragmentContext, String inputPath, FileSystem fileSystem) {
    this(fragmentContext, inputPath, fileSystem, DEFAULT_LENGTH);
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
      JsonFactory factory = new JsonFactory();
      parser = factory.createJsonParser(fileSystem.open(hadoopPath));
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
      while (ReadType.OBJECT.readRecord(this, null, nextRowIndex++, 0)) {
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
        outputMutator.removeField(field.getAsMaterializedField());
      }

      if (diffSchema.isChanged()) {
        outputMutator.setNewSchema();
      }


    } catch (IOException | SchemaChangeException e) {
      logger.error("Error reading next in Json reader", e);
    }

    for (VectorHolder holder : valueVectorMap.values()) {
      holder.populateVectorLength();
    }

    return nextRowIndex;
  }

  private void resetBatch() {
    for (VectorHolder value : valueVectorMap.values()) {
      value.reset();
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


  private RecordSchema getCurrentSchema() {
    return currentSchema;
  }

  private void setCurrentSchema(RecordSchema schema) {
    currentSchema = schema;
  }

  private List<Field> getRemovedFields() {
    return removedFields;
  }

  public BufferAllocator getAllocator() {
    return allocator;
  }

  public static enum ReadType {
    ARRAY(END_ARRAY) {
      @Override
      public Field createField(RecordSchema parentSchema, String prefixFieldName, String fieldName, MajorType fieldType, int index) {
        return new NamedField(parentSchema, prefixFieldName, fieldName, fieldType);
      }

      @Override
      public RecordSchema createSchema() throws IOException {
        return new ObjectSchema();
      }
    },
    OBJECT(END_OBJECT) {
      @Override
      public Field createField(RecordSchema parentSchema,
                               String prefixFieldName,
                               String fieldName,
                               MajorType fieldType,
                               int index) {
        return new NamedField(parentSchema, prefixFieldName, fieldName, fieldType);
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

    @SuppressWarnings("ConstantConditions")
    public boolean readRecord(JSONRecordReader reader,
                              String prefixFieldName,
                              int rowIndex,
                              int groupCount) throws IOException, SchemaChangeException {
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
        MajorType fieldType = JacksonHelper.getFieldType(token, this == ReadType.ARRAY);
        ReadType readType = null;
        switch (token) {
          case START_ARRAY:
            readType = ReadType.ARRAY;
            groupCount++;
            break;
          case START_OBJECT:
            readType = ReadType.OBJECT;
            groupCount = 0;
            break;
        }

        if (fieldType != null) { // Including nulls
          boolean currentFieldFull = !recordData(
              readType,
              reader,
              fieldType,
              prefixFieldName,
              fieldName,
              rowIndex,
              colIndex,
              groupCount);

          isFull = isFull || currentFieldFull;
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

    private boolean recordData(JSONRecordReader.ReadType readType,
                               JSONRecordReader reader,
                               MajorType fieldType,
                               String prefixFieldName,
                               String fieldName,
                               int rowIndex,
                               int colIndex,
                               int groupCount) throws IOException, SchemaChangeException {
      RecordSchema currentSchema = reader.getCurrentSchema();
      Field field = currentSchema.getField(fieldName == null ? prefixFieldName : fieldName, colIndex);
      boolean isFieldFound = field != null;
      List<Field> removedFields = reader.getRemovedFields();
      boolean newFieldLateBound = fieldType.getMinorType().equals(MinorType.LATE);

      if (isFieldFound && !field.getFieldType().equals(fieldType)) {
        boolean existingFieldLateBound = field.getFieldType().getMinorType().equals(MinorType.LATE);

        if (newFieldLateBound && !existingFieldLateBound) {
          fieldType = Types.overrideMinorType(fieldType, field.getFieldType().getMinorType());
        } else if (!newFieldLateBound && existingFieldLateBound) {
          field.setFieldType(Types.overrideMinorType(field.getFieldType(), fieldType.getMinorType()));
        } else if (!newFieldLateBound && !existingFieldLateBound) {
          if (field.hasSchema()) {
            removeChildFields(removedFields, field);
          }
          removedFields.add(field);
          currentSchema.removeField(field, colIndex);

          isFieldFound = false;
        }
      }

      if (!isFieldFound) {
        field = createField(
            currentSchema,
            prefixFieldName,
            fieldName,
            fieldType,
            colIndex
        );

        reader.recordNewField(field);
        currentSchema.addField(field);
      }

      field.setRead(true);

      VectorHolder holder = getOrCreateVectorHolder(reader, field);
      if (readType != null) {
        RecordSchema fieldSchema = field.getAssignedSchema();
        RecordSchema newSchema = readType.createSchema();

        if (readType != ReadType.ARRAY) {
          reader.setCurrentSchema(fieldSchema);
          if (fieldSchema == null) reader.setCurrentSchema(newSchema);
          readType.readRecord(reader, field.getFullFieldName(), rowIndex, groupCount);
        } else {
          readType.readRecord(reader, field.getFullFieldName(), rowIndex, groupCount);
        }

        reader.setCurrentSchema(currentSchema);

      } else if (holder != null && !newFieldLateBound && fieldType.getMinorType() != MinorType.LATE) {
        return addValueToVector(
            rowIndex,
            holder,
            JacksonHelper.getValueFromFieldType(
                reader.getParser(),
                fieldType.getMinorType()
            ),
            fieldType.getMinorType(),
            groupCount
        );
      }

      return true;
    }

    private static <T> boolean addValueToVector(int index, VectorHolder holder, T val, MinorType minorType, int groupCount) {
      switch (minorType) {
        case INT: {
          holder.incAndCheckLength(NullableIntHolder.WIDTH * 8 + 1);
          if (groupCount == 0) {
            if (val != null) {
              NullableIntVector int4 = (NullableIntVector) holder.getValueVector();
              NullableIntVector.Mutator m = int4.getMutator();
              m.set(index, (Integer) val);
            }
          } else {
            if (val == null) {
              throw new UnsupportedOperationException("Nullable repeated int is not supported.");
            }

            RepeatedIntVector repeatedInt4 = (RepeatedIntVector) holder.getValueVector();
            RepeatedIntVector.Mutator m = repeatedInt4.getMutator();
            holder.setGroupCount(index);
            m.add(index, (Integer) val);
          }

          return holder.hasEnoughSpace(NullableIntHolder.WIDTH * 8 + 1);
        }
        case FLOAT4: {
          holder.incAndCheckLength(NullableFloat4Holder.WIDTH * 8 + 1);
          if (groupCount == 0) {
            if (val != null) {
              NullableFloat4Vector float4 = (NullableFloat4Vector) holder.getValueVector();
              NullableFloat4Vector.Mutator m = float4.getMutator();
              m.set(index, (Float) val);
            }
          } else {
            if (val == null) {
              throw new UnsupportedOperationException("Nullable repeated float is not supported.");
            }

            RepeatedFloat4Vector repeatedFloat4 = (RepeatedFloat4Vector) holder.getValueVector();
            RepeatedFloat4Vector.Mutator m = repeatedFloat4.getMutator();
            holder.setGroupCount(index);
            m.add(index, (Float) val);
          }
          return holder.hasEnoughSpace(NullableFloat4Holder.WIDTH * 8 + 1);
        }
        case VARCHAR: {
          if (val == null) {
            return (index + 1) * 4 <= holder.getLength();
          } else {
            byte[] bytes = ((String) val).getBytes(UTF_8);
            int length = bytes.length;
            holder.incAndCheckLength(length);
            if (groupCount == 0) {
              NullableVarCharVector varLen4 = (NullableVarCharVector) holder.getValueVector();
              NullableVarCharVector.Mutator m = varLen4.getMutator();
              m.set(index, bytes);
            } else {
              RepeatedVarCharVector repeatedVarLen4 = (RepeatedVarCharVector) holder.getValueVector();
              RepeatedVarCharVector.Mutator m = repeatedVarLen4.getMutator();
              holder.setGroupCount(index);
              m.add(index, bytes);
            }
            return holder.hasEnoughSpace(length + 4 + 1);
          }
        }
        case BIT: {
          holder.incAndCheckLength(NullableBitHolder.WIDTH + 1);
          if (groupCount == 0) {
            if (val != null) {
              NullableBitVector bit = (NullableBitVector) holder.getValueVector();
              NullableBitVector.Mutator m = bit.getMutator();
              m.set(index, (Boolean) val ? 1 : 0);
            }
          } else {
            if (val == null) {
              throw new UnsupportedOperationException("Nullable repeated boolean is not supported.");
            }

            RepeatedBitVector repeatedBit = (RepeatedBitVector) holder.getValueVector();
            RepeatedBitVector.Mutator m = repeatedBit.getMutator();
            holder.setGroupCount(index);
            m.add(index, (Boolean) val ? 1 : 0);
          }
          return holder.hasEnoughSpace(NullableBitHolder.WIDTH + 1);
        }
        default:
          throw new DrillRuntimeException("Type not supported to add value. Type: " + minorType);
      }
    }

    private VectorHolder getOrCreateVectorHolder(JSONRecordReader reader, Field field) throws SchemaChangeException {
      return reader.getOrCreateVectorHolder(field);
    }

    public abstract RecordSchema createSchema() throws IOException;

    public abstract Field createField(RecordSchema parentSchema,
                                      String prefixFieldName,
                                      String fieldName,
                                      MajorType fieldType,
                                      int index);
  }

  private void recordNewField(Field field) {
    diffSchema.recordNewField(field);
  }

  private VectorHolder getOrCreateVectorHolder(Field field) throws SchemaChangeException {
    String fullFieldName = field.getFullFieldName();
    VectorHolder holder = valueVectorMap.get(fullFieldName);

    if (holder == null) {
      MajorType type = field.getFieldType();
      MinorType minorType = type.getMinorType();

      if (minorType.equals(MinorType.MAP) || minorType.equals(MinorType.LATE)) {
        return null;
      }

      MaterializedField f = MaterializedField.create(new SchemaPath(fullFieldName, ExpressionPosition.UNKNOWN), type);

      ValueVector v = TypeHelper.getNewVector(f, allocator);
      AllocationHelper.allocate(v, batchSize, 50);
      holder = new VectorHolder(v);
      valueVectorMap.put(fullFieldName, holder);
      outputMutator.addField(v);
      return holder;
    }
    return holder;
  }
}
