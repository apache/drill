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
package org.apache.drill.exec.planner.sql.handlers;

import com.google.common.collect.Lists;
import com.google.common.io.Files;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.commons.io.FileUtils;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.exception.FunctionValidationException;
import org.apache.drill.exec.exception.VersionMismatchException;
import org.apache.drill.exec.expr.fn.RemoteFunctionRegistry;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.planner.sql.DirectPlan;
import org.apache.drill.exec.planner.sql.parser.SqlCreateFunction;
import org.apache.drill.exec.proto.UserBitShared.Func;
import org.apache.drill.exec.proto.UserBitShared.Jar;
import org.apache.drill.exec.proto.UserBitShared.Registry;
import org.apache.drill.exec.store.sys.store.DataChangeVersion;
import org.apache.drill.exec.util.JarUtil;
import org.apache.drill.exec.work.foreman.ForemanSetupException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.UUID;

public class CreateFunctionHandler extends DefaultSqlHandler {

  private static org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CreateFunctionHandler.class);

  public CreateFunctionHandler(SqlHandlerConfig config) {
    super(config);
  }

  /**
   * Creates UDFs dynamically.
   *
   * @return - Single row indicating list of registered UDFs, raise exception otherwise
   */
  @Override
  public PhysicalPlan getPlan(SqlNode sqlNode) throws ForemanSetupException, IOException {
    if (context.getOption(ExecConstants.DYNAMIC_UDF_SUPPORT_ENABLED).bool_val) {
      SqlCreateFunction node = unwrap(sqlNode, SqlCreateFunction.class);
      String jarName = ((SqlCharStringLiteral) node.getJar()).toValue();
      String sourceName = JarUtil.getSourceName(jarName);

      RemoteFunctionRegistry remoteRegistry = context.getRemoteFunctionRegistry();
      FileSystem fs = remoteRegistry.getFs();
      Path tmpDir = new Path(remoteRegistry.getTmpArea(), UUID.randomUUID().toString());
      File localTmpDir = Files.createTempDir();

      boolean inProgress = false;
      try {
        final String action = remoteRegistry.addToJars(jarName, RemoteFunctionRegistry.Action.REGISTRATION);
        if (!(inProgress = action == null)) {
          return DirectPlan.createDirectPlan(context, false,
              String.format("Jar with %s name is used. Action: %s", jarName, action));
        }

        // verify that binary and source exist
        Path remoteBinary = new Path(remoteRegistry.getStagingArea(), jarName);
        Path remoteSource = new Path(remoteRegistry.getStagingArea(), sourceName);
        if (!fs.exists(remoteBinary) || !fs.exists(remoteSource)) {
          return DirectPlan.createDirectPlan(context, false,
              String.format("Binary [%s] or source [%s] is absent in udf staging area [%s].", jarName, sourceName, remoteRegistry.getStagingArea().toUri().getPath()));
        }

        // backup binary & source (copy to udf tmp directory)
        fs.mkdirs(tmpDir);
        Path tmpBinary = new Path(tmpDir, jarName);
        Path tmpSource = new Path(tmpDir, sourceName);

        FileUtil.copy(fs, remoteBinary, fs, tmpBinary, false, fs.getConf());
        FileUtil.copy(fs, remoteSource, fs, tmpSource, false, fs.getConf());

        // copy binary to local fs, we don't need source for validation
        Path localBinary = new Path(new Path(localTmpDir.toURI()), jarName);
        fs.copyToLocalFile(tmpBinary, localBinary);

        // validate functions locally
        List<Func> functions;
        try {
          functions = context.getFunctionRegistry().validate(localBinary);
        } catch (FunctionValidationException ex) {
          return DirectPlan.createDirectPlan(context, false, ex.getMessage());
        }

        if (functions.size() == 0) {
          return DirectPlan.createDirectPlan(context, false,
              String.format("Jar %s does not contain functions", jarName));
        }

        // validate and register remotely
        Jar jar = Jar.newBuilder().setName(jarName).addAllFunction(functions).build();
        String error = register(remoteRegistry, jar, tmpBinary, tmpSource, remoteRegistry.getRetryTimes());

        if (error != null) {
          return DirectPlan.createDirectPlan(context, false, error);
        }

        // remove jars from staging area
        try {
          fs.delete(remoteBinary, false);
          fs.delete(remoteSource, false);
        } catch (IOException ex) {
          logger.warn("Failed to delete binary {} and source {} from staging area", jarName, sourceName);
        }

        return DirectPlan.createDirectPlan(context, true,
            String.format("The following UDFs in jar %s have been registered:\n%s", jarName, functions));

      } finally {
        if (inProgress) {
          remoteRegistry.removeFromJars(jarName);
        }
        FileUtils.deleteQuietly(localTmpDir);
        if (fs.exists(tmpDir)) {
          fs.delete(tmpDir, true);
        }
      }
    }
    throw UserException.validationError()
        .message("Dynamic UDFs support is disabled.")
        .build(logger);
  }

  private String validate(Registry registry, Jar jar) {
    for (Jar remoteJar : registry.getJarList()) {
      if (remoteJar.getName().equals(jar.getName())) {
        return String.format("Jar with %s name has been already registered", jar.getName());
      }
      for (Func remoteFunction : remoteJar.getFunctionList()) {
        for (Func func : jar.getFunctionList()) {
          if (remoteFunction.getName().equals(func.getName()) && remoteFunction.getMajorTypeList().equals(func.getMajorTypeList())) {
            return String.format("Found duplicated function in %s - %s", remoteJar.getName(), remoteFunction);
          }
        }
      }
    }
    return null;
  }

  private String register(RemoteFunctionRegistry remoteFunctionRegistry, Jar jar, Path tmpBinary, Path tmpSource, int retryTimes) throws IOException {
    DataChangeVersion version = new DataChangeVersion();
    Registry registry = remoteFunctionRegistry.getRegistry(version);
    // validate against remote registry
    String error = validate(registry, jar);
    if (error == null) {
      // copy jars to registry area
      FileSystem fs = remoteFunctionRegistry.getFs();
      Path binary = new Path(remoteFunctionRegistry.getRegistryArea(), tmpBinary.getName());
      Path source = new Path(remoteFunctionRegistry.getRegistryArea(), tmpSource.getName());
      FileUtil.copy(fs, tmpBinary, fs, binary, false, true, fs.getConf());
      FileUtil.copy(fs, tmpSource, fs, source, false, true, fs.getConf());

      // add jar info into remote registry if all validation has passed
      List<Jar> remoteJars = Lists.newArrayList(registry.getJarList());
      remoteJars.add(jar);
      Registry updatedRegistry = Registry.newBuilder().addAllJar(remoteJars).build();
      try {
        remoteFunctionRegistry.updateRegistry(updatedRegistry, version);
      } catch (VersionMismatchException ex) {
        if (retryTimes-- == 0) {
          fs.delete(binary, false);
          fs.delete(source, false);
          throw new DrillRuntimeException("Failed to update remote function registry. Exceeded retry times limit.");
        }
        register(remoteFunctionRegistry, jar, tmpBinary, tmpSource, retryTimes);
      }
    }
    return error;
  }
}