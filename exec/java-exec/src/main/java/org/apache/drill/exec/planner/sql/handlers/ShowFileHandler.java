/**
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
 */
package org.apache.drill.exec.planner.sql.handlers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import net.hydromatic.optiq.SchemaPlus;
import net.hydromatic.optiq.tools.Planner;
import net.hydromatic.optiq.tools.RelConversionException;
import net.hydromatic.optiq.tools.ValidationException;

import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.planner.sql.DirectPlan;
import org.apache.drill.exec.planner.sql.parser.SqlShowFiles;
import org.apache.drill.exec.store.AbstractSchema;
import org.apache.drill.exec.store.dfs.WorkspaceSchemaFactory.WorkspaceSchema;
import org.apache.drill.exec.store.dfs.shim.DrillFileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.eigenbase.relopt.hep.HepPlanner;
import org.eigenbase.sql.SqlIdentifier;
import org.eigenbase.sql.SqlNode;


public class ShowFileHandler extends DefaultSqlHandler {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SetOptionHandler.class);

  public ShowFileHandler(SqlHandlerConfig config) {
    super(config);
  }

  @Override
  public PhysicalPlan getPlan(SqlNode sqlNode) throws ValidationException, RelConversionException, IOException {

    SqlIdentifier from = ((SqlShowFiles) sqlNode).getDb();

    DrillFileSystem fs = null;
    String defaultLocation = null;
    String fromDir = "./";

    try {
      SchemaPlus defaultSchema = context.getNewDefaultSchema();
      SchemaPlus drillSchema = defaultSchema;

      // Show files can be used without from clause, in which case we display the files in the default schema
      if (from != null) {
        // We are not sure if the full from clause is just the schema or includes table name, first try to see if the full path specified is a schema
        try {
          drillSchema = findSchema(context.getRootSchema(), defaultSchema, from.names);
        } catch (Exception e) {
            // Entire from clause is not a schema, try to obtain the schema without the last part of the specified clause.
            drillSchema = findSchema(context.getRootSchema(), defaultSchema, from.names.subList(0, from.names.size() - 1));
            fromDir = fromDir + from.names.get((from.names.size() - 1));
        }
      }

      AbstractSchema tempSchema = getDrillSchema(drillSchema);
      WorkspaceSchema schema = null;
      if (tempSchema instanceof WorkspaceSchema) {
        schema = ((WorkspaceSchema)tempSchema);
      } else {
        throw new ValidationException("Unsupported schema");
      }

      // Get the file system object
      fs = schema.getFS();

      // Get the default path
      defaultLocation = schema.getDefaultLocation();
    } catch (Exception e) {
        if (from == null) {
          return DirectPlan.createDirectPlan(context, false, "Show files without FROM / IN clause can be used only after specifying a default file system schema");
        }
        return DirectPlan.createDirectPlan(context, false, String.format("Current schema '%s' is not a file system schema. " +
                                           "Can't execute show files on this schema.", from.toString()));
    }

    List<ShowFilesCommandResult> rows = new ArrayList<>();

    for (FileStatus fileStatus : fs.list(false, new Path(defaultLocation, fromDir))) {
      ShowFilesCommandResult result = new ShowFilesCommandResult(fileStatus.getPath().getName(), fileStatus.isDir(),
                                                                 !fileStatus.isDir(), fileStatus.getLen(),
                                                                 fileStatus.getOwner(), fileStatus.getGroup(),
                                                                 fileStatus.getPermission().toString(),
                                                                 fileStatus.getAccessTime(), fileStatus.getModificationTime());
      rows.add(result);
    }
    return DirectPlan.createDirectPlan(context.getCurrentEndpoint(), rows.iterator(), ShowFilesCommandResult.class);
  }
}
