package org.apache.drill.exec.store.parquet;

import static parquet.column.Encoding.PLAIN;

import java.util.HashMap;
import java.util.Map;

import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.store.ByteArrayUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import parquet.bytes.BytesInput;
import parquet.column.ColumnDescriptor;
import parquet.hadoop.ParquetFileWriter;
import parquet.hadoop.metadata.CompressionCodecName;
import parquet.schema.MessageType;
import parquet.schema.MessageTypeParser;

public class TestFileGenerator {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestFileGenerator.class);

  

  // 10 mb per page
  static int bytesPerPage = 1024 * 1024 * 1;
  // { 00000001, 00000010, 00000100, 00001000, 00010000, ... }
  static byte[] bitFields = { 1, 2, 4, 8, 16, 32, 64, -128 };
  static final byte allBitsTrue = -1;
  static final byte allBitsFalse = 0;
  static final byte[] varLen1 = { 50, 51, 52, 53, 54, 55, 56 };
  static final byte[] varLen2 = { 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1 };
  static final byte[] varLen3 = { 100, 99, 98 };

  static final Object[] intVals = { -200, 100, Integer.MAX_VALUE };
  static final Object[] longVals = { -5000l, 5000l, Long.MAX_VALUE };
  static final Object[] floatVals = { 1.74f, Float.MAX_VALUE, Float.MIN_VALUE };
  static final Object[] doubleVals = { 100.45d, Double.MAX_VALUE, Double.MIN_VALUE, };
  static final Object[] boolVals = { false, false, true };
  static final Object[] binVals = { varLen1, varLen2, varLen3 };
  static final Object[] bin2Vals = { varLen3, varLen2, varLen1 };

  static class FieldInfo {

    String parquetType;
    String name;
    int bitLength;
    int numberOfPages;
    Object[] values;
    TypeProtos.MinorType type;

    FieldInfo(int recordsPerRowGroup, String parquetType, String name, int bitLength, Object[] values, TypeProtos.MinorType type) {
      this.parquetType = parquetType;
      this.name = name;
      this.bitLength = bitLength;
      this.numberOfPages = Math.max(1, (int) Math.ceil( ((long) recordsPerRowGroup) * bitLength / 8.0 / bytesPerPage));
      this.values = values;
      // generator is designed to use 3 values
      assert values.length == 3;
      this.type = type;
    }
  }

  private static class WrapAroundCounter {

    int maxVal;
    int val;

    public WrapAroundCounter(int maxVal) {
      this.maxVal = maxVal;
    }

    public int increment() {
      val++;
      if (val > maxVal) {
        val = 0;
      }
      return val;
    }

    public void reset() {
      val = 0;
    }

  }

  public static HashMap<String, FieldInfo> getFieldMap(int recordsPerRowGroup) {
    HashMap<String, FieldInfo> fields = new HashMap<>();
    fields.put("integer", new FieldInfo(recordsPerRowGroup, "int32", "integer", 32, intVals, TypeProtos.MinorType.INT));
    fields.put("bigInt", new FieldInfo(recordsPerRowGroup, "int64", "bigInt", 64, longVals, TypeProtos.MinorType.BIGINT));
    fields.put("f", new FieldInfo(recordsPerRowGroup, "float", "f", 32, floatVals, TypeProtos.MinorType.FLOAT4));
    fields.put("d", new FieldInfo(recordsPerRowGroup, "double", "d", 64, doubleVals, TypeProtos.MinorType.FLOAT8));
    fields.put("b", new FieldInfo(recordsPerRowGroup, "boolean", "b", 1, boolVals, TypeProtos.MinorType.BIT));
    fields.put("bin", new FieldInfo(recordsPerRowGroup, "binary", "bin", -1, binVals, TypeProtos.MinorType.VARBINARY));
    fields.put("bin2", new FieldInfo(recordsPerRowGroup, "binary", "bin2", -1, bin2Vals, TypeProtos.MinorType.VARBINARY));
    return fields;
  }

  public static void generateParquetFile(String filename, int numberRowGroups, int recordsPerRowGroup) throws Exception {
    final Map<String, FieldInfo> fields = getFieldMap(recordsPerRowGroup);

    int currentBooleanByte = 0;
    WrapAroundCounter booleanBitCounter = new WrapAroundCounter(7);
    
    Configuration configuration = new Configuration();
    configuration.set(ParquetSchemaProvider.HADOOP_DEFAULT_NAME, "file:///");
    // "message m { required int32 integer; required int64 integer64; required boolean b; required float f; required double d;}"

    FileSystem fs = FileSystem.get(configuration);
    Path path = new Path(filename);
    if (fs.exists(path))
      fs.delete(path, false);

    String messageSchema = "message m {";
    for (FieldInfo fieldInfo : fields.values()) {
      messageSchema += " required " + fieldInfo.parquetType + " " + fieldInfo.name + ";";
    }
    // remove the last semicolon, java really needs a join method for strings...
    // TODO - nvm apparently it requires a semicolon after every field decl, might want to file a bug
    // messageSchema = messageSchema.substring(schemaType, messageSchema.length() - 1);
    messageSchema += "}";

    MessageType schema = MessageTypeParser.parseMessageType(messageSchema);

    CompressionCodecName codec = CompressionCodecName.UNCOMPRESSED;
    ParquetFileWriter w = new ParquetFileWriter(configuration, schema, path);
    w.start();
    HashMap<String, Integer> columnValuesWritten = new HashMap();
    int valsWritten;
    for (int k = 0; k < numberRowGroups; k++) {
      w.startBlock(1);
      currentBooleanByte = 0;
      booleanBitCounter.reset();

      for (FieldInfo fieldInfo : fields.values()) {

        if (!columnValuesWritten.containsKey(fieldInfo.name)) {
          columnValuesWritten.put((String) fieldInfo.name, 0);
          valsWritten = 0;
        } else {
          valsWritten = columnValuesWritten.get(fieldInfo.name);
        }

        String[] path1 = { (String) fieldInfo.name };
        ColumnDescriptor c1 = schema.getColumnDescription(path1);

        w.startColumn(c1, recordsPerRowGroup, codec);
        int valsPerPage = (int) Math.ceil(recordsPerRowGroup / (float) fieldInfo.numberOfPages);
        byte[] bytes;
        // for variable length binary fields
        int bytesNeededToEncodeLength = 4;
        if ((int) fieldInfo.bitLength > 0) {
          bytes = new byte[(int) Math.ceil(valsPerPage * (int) fieldInfo.bitLength / 8.0)];
        } else {
          // the twelve at the end is to account for storing a 4 byte length with each value
          int totalValLength = ((byte[]) fieldInfo.values[0]).length + ((byte[]) fieldInfo.values[1]).length
              + ((byte[]) fieldInfo.values[2]).length + 3 * bytesNeededToEncodeLength;
          // used for the case where there is a number of values in this row group that is not divisible by 3
          int leftOverBytes = 0;
          if (valsPerPage % 3 > 0)
            leftOverBytes += ((byte[]) fieldInfo.values[1]).length + 4;
          if (valsPerPage % 3 > 1)
            leftOverBytes += ((byte[]) fieldInfo.values[2]).length + 4;
          bytes = new byte[valsPerPage / 3 * totalValLength + leftOverBytes];
        }
        int bytesPerPage = (int) (valsPerPage * ((int) fieldInfo.bitLength / 8.0));
        int bytesWritten = 0;
        for (int z = 0; z < (int) fieldInfo.numberOfPages; z++, bytesWritten = 0) {
          for (int i = 0; i < valsPerPage; i++) {
            // System.out.print(i + ", " + (i % 25 == 0 ? "\n gen " + fieldInfo.name + ": " : ""));
            if (fieldInfo.values[0] instanceof Boolean) {

              bytes[currentBooleanByte] |= bitFields[booleanBitCounter.val]
                  & ((boolean) fieldInfo.values[valsWritten % 3] ? allBitsTrue : allBitsFalse);
              booleanBitCounter.increment();
              if (booleanBitCounter.val == 0) {
                currentBooleanByte++;
              }
              valsWritten++;
              if (currentBooleanByte > bytesPerPage)
                break;
            } else {
              if (fieldInfo.values[valsWritten % 3] instanceof byte[]) {
                System.arraycopy(ByteArrayUtil.toByta(((byte[]) fieldInfo.values[valsWritten % 3]).length), 0, bytes,
                    bytesWritten, bytesNeededToEncodeLength);
                System.arraycopy(fieldInfo.values[valsWritten % 3], 0, bytes, bytesWritten + bytesNeededToEncodeLength,
                    ((byte[]) fieldInfo.values[valsWritten % 3]).length);
                bytesWritten += ((byte[]) fieldInfo.values[valsWritten % 3]).length + bytesNeededToEncodeLength;
              } else {
                System.arraycopy(ByteArrayUtil.toByta(fieldInfo.values[valsWritten % 3]), 0, bytes, i
                    * ((int) fieldInfo.bitLength / 8), (int) fieldInfo.bitLength / 8);
              }
              valsWritten++;
            }

          }
          w.writeDataPage((int) (recordsPerRowGroup / (int) fieldInfo.numberOfPages), bytes.length,
              BytesInput.from(bytes), PLAIN, PLAIN, PLAIN);
          currentBooleanByte = 0;
        }
        w.endColumn();
        columnValuesWritten.remove((String) fieldInfo.name);
        columnValuesWritten.put((String) fieldInfo.name, valsWritten);
      }

      w.endBlock();
    }
    w.end(new HashMap<String, String>());
    logger.debug("Finished generating parquet file.");
  }

}
