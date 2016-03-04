/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.version;

import com.google.common.base.Preconditions;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/**
 * This class is called from the exec->vector module using Maven-exec.
 * To make use of the class, it needs to be compiled in an earlier module.
 * For the configuration see the pom.xml file in that module.
 */
public class Generator {

  public static void main(String[] args) {
    String toReplace = "REPLACE_WITH_DRILL_VERSION";
    String template =
        "/**\n" +
        " * Licensed to the Apache Software Foundation (ASF) under one\n" +
        " * or more contributor license agreements.  See the NOTICE file\n" +
        " * distributed with this work for additional information\n" +
        " * regarding copyright ownership.  The ASF licenses this file\n" +
        " * to you under the Apache License, Version 2.0 (the\n" +
        " * \"License\"); you may not use this file except in compliance\n" +
        " * with the License.  You may obtain a copy of the License at\n" +
        " *\n" +
        " * http://www.apache.org/licenses/LICENSE-2.0\n" +
        " *\n" +
        " * Unless required by applicable law or agreed to in writing, software\n" +
        " * distributed under the License is distributed on an \"AS IS\" BASIS,\n" +
        " * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.\n" +
        " * See the License for the specific language governing permissions and\n" +
        " * limitations under the License.\n" +
        " */\n" +
        "package org.apache.drill.common.util;\n" +
        "\n" +
        "/**\n" +
        " * Get access to the Drill Version\n" +
        " */\n" +
        "// File generated during build, DO NOT EDIT!!\n" +
        "public class DrillVersionInfo {\n" +
        "\n" +
        "  /**\n" +
        "   * Get the Drill version from the Manifest file\n" +
        "   * @return the version number as x.y.z\n" +
        "   */\n" +
        "  public static String getVersion() {\n" +
        "    return \"" + toReplace + "\";\n" +
        "  }\n" +
        "}\n";
    Preconditions.checkArgument(args.length == 2,
        "Two arguments expected, the first is the target java source directory for the generated file" +
            " and the second is the Drill version.");
    File srcFile = new File(args[0] + "/org/apache/drill/common/util/DrillVersionInfo.java");
    srcFile = srcFile.getAbsoluteFile();
    File parent = srcFile.getParentFile();
    if (!parent.exists()) {
      if (!parent.mkdirs()) {
        throw new RuntimeException("Error generating Drill version info class. Couldn't mkdirs for " + parent);
      }
    }
    final FileWriter writer;
    try {
      writer = new FileWriter(srcFile);
      writer.write(template.replace(toReplace, args[1]));
      writer.close();
    } catch (IOException e) {
      throw new RuntimeException("Error generating Drill version info class. " +
          "Couldn't open source file for writing: " + srcFile);
    }
  }
}
