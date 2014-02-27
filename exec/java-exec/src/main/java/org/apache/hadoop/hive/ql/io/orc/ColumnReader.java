package org.apache.hadoop.hive.ql.io.orc;

import org.apache.drill.exec.vector.ValueVector;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public abstract class ColumnReader {
    ValueVector valueVector;
    int columnId;

    ColumnReader(int columnId, ValueVector valueVector) {
        this.columnId = columnId;
        this.valueVector = valueVector;
    }

    public void readStripe(Map<StreamName, InStream> streams, List<OrcProto.ColumnEncoding> encodings) throws IOException {
        InStream in = streams.get(new StreamName(columnId,
                OrcProto.Stream.Kind.PRESENT));
    }
}
