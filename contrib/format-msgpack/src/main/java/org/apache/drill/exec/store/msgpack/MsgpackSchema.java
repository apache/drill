package org.apache.drill.exec.store.msgpack;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.exception.SchemaChangeRuntimeException;
import org.apache.drill.exec.proto.UserBitShared.SerializedField;
import org.apache.drill.exec.proto.UserBitShared.SerializedField.Builder;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.AccessControlException;

import com.google.protobuf.TextFormat;
import com.google.protobuf.TextFormat.ParseException;

public class MsgpackSchema {
  @SuppressWarnings("unused")
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MsgpackSchema.class);

  private DrillFileSystem fileSystem;

  public MsgpackSchema(DrillFileSystem fileSystem) {
    this.fileSystem = fileSystem;
  }

  public MaterializedField load(Path schemaLocation) throws AccessControlException, FileNotFoundException, IOException {
    MaterializedField previousMapField = null;
    if (fileSystem.exists(schemaLocation)) {
      try (FSDataInputStream in = fileSystem.open(schemaLocation)) {
        String schemaData = IOUtils.toString(in);
        Builder newBuilder = SerializedField.newBuilder();
        try {
          TextFormat.merge(schemaData, newBuilder);
        } catch (ParseException e) {
          throw new DrillRuntimeException("Failed to read schema file: " + schemaLocation, e);
        }
        SerializedField read = newBuilder.build();
        previousMapField = MaterializedField.create(read);
      }
    }
    return previousMapField;
  }

  public void save(MaterializedField mapField, Path schemaLocation) throws IOException {
    try (FSDataOutputStream out = fileSystem.create(schemaLocation, true)) {
      SerializedField serializedMapField = mapField.getSerializedField();
      String data = TextFormat.printToString(serializedMapField);
      IOUtils.write(data, out);
    }
  }

  public MaterializedField merge(MaterializedField existingField, MaterializedField newField) {
    Preconditions.checkArgument(existingField.getType().getMinorType() == MinorType.MAP,
        "Field " + existingField + " is not a MAP type.");
    Preconditions.checkArgument(existingField.hasSameTypeAndMode(newField),
        "Field " + existingField + " and " + newField + " not same.");
    MaterializedField merged = existingField.clone();
    privateMerge(merged, newField);
    return merged;
  }

  private void privateMerge(MaterializedField existingField, MaterializedField newField) {
    Preconditions.checkArgument(existingField.getType().getMinorType() == MinorType.MAP,
        "Field " + existingField + " is not a MAP type.");
    for (MaterializedField newChild : newField.getChildren()) {
      String newChildName = newChild.getName();
      MaterializedField foundExistingChild = getFieldByName(newChildName, existingField);
      if (foundExistingChild != null) {
        if (foundExistingChild.hasSameTypeAndMode(newChild)) {
          if (foundExistingChild.getType().getMinorType() == MinorType.MAP) {
            privateMerge(foundExistingChild, newChild);
          } // else we already have it
        } else {
          // error
          throw new SchemaChangeRuntimeException("Not the same schema for " + foundExistingChild + " and " + newChild);
        }
      } else {
        existingField.addChild(newChild.clone());
      }
    }
  }

  private MaterializedField getFieldByName(String newChildName, MaterializedField existingField) {
    for (MaterializedField f : existingField.getChildren()) {
      if (newChildName.equalsIgnoreCase(f.getName())) {
        return f;
      }
    }
    return null;
  }
}
