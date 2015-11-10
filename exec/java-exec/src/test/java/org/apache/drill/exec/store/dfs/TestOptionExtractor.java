package org.apache.drill.exec.store.dfs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.Collection;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.scanner.RunTimeScan;
import org.apache.drill.common.scanner.persistence.ScanResult;
import org.apache.drill.exec.store.dfs.WorkspaceSchemaFactory.OptionExtractor;
import org.apache.drill.exec.store.dfs.WorkspaceSchemaFactory.OptionsDescriptor;
import org.apache.drill.exec.store.easy.text.TextFormatPlugin.TextFormatConfig;
import org.junit.Test;

import com.fasterxml.jackson.annotation.JsonTypeName;


public class TestOptionExtractor {

  @Test
  public void test() {
    DrillConfig config = DrillConfig.create();
    ScanResult scanResult = RunTimeScan.fromPrescan(config);
    OptionExtractor e = new OptionExtractor(scanResult);
    Collection<OptionsDescriptor> options = e.getOptions();
    for (OptionsDescriptor d : options) {
      assertEquals(d.pluginConfigClass.getAnnotation(JsonTypeName.class).value(), d.typeName);
      switch (d.typeName) {
        case "text":
          assertEquals(TextFormatConfig.class, d.pluginConfigClass);
          assertEquals(
              "(type: String, lineDelimiter: String, fieldDelimiter: String, quote: String, escape: String, comment: String, skipFirstLine: boolean, extractHeader: boolean)",
              d.presentParams()
          );
          break;
        case "named":
          assertEquals(NamedFormatPluginConfig.class, d.pluginConfigClass);
          assertEquals("(type: String, name: String)", d.presentParams());
          break;
        case "json":
        case "sequencefile":
        case "parquet":
        case "avro":
          assertEquals(d.typeName, "(type: String)", d.presentParams());
          break;
        default:
          fail("add validation for format plugin type " + d.typeName);
      }
    }
  }
}
