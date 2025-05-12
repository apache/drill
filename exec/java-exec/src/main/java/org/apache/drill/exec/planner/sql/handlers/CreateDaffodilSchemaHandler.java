package org.apache.drill.exec.planner.sql.handlers;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.commons.io.FileUtils;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.util.DrillFileUtils;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.exception.VersionMismatchException;
import org.apache.drill.exec.expr.fn.registry.RemoteFunctionRegistry;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.planner.sql.DirectPlan;
import org.apache.drill.exec.planner.sql.parser.SqlCreateDaffodilSchema;
import org.apache.drill.exec.proto.UserBitShared.Jar;
import org.apache.drill.exec.proto.UserBitShared.Registry;
import org.apache.drill.exec.store.sys.store.DataChangeVersion;
import org.apache.drill.exec.util.ImpersonationUtil;
import org.apache.drill.exec.util.JarUtil;
import org.apache.drill.exec.work.foreman.ForemanSetupException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.UUID;

public class CreateDaffodilSchemaHandler extends DefaultSqlHandler {

  private static final Logger logger = LoggerFactory.getLogger(CreateDaffodilSchemaHandler.class);

  private int retryAttempts;
  private final FileSystem fs;
  private final Path registryArea;
  private final Path stagingArea;
  private final Path tmpArea;
  private final String root;

  public CreateDaffodilSchemaHandler(SqlHandlerConfig config) {
    super(config);

    // Get the paths where the DFDL shemata files will be stored.
    DrillConfig drillConfig = config.getContext().getDrillbitContext().getConfig();

    logger.info("Preparing three daffodil staging areas: staging, registry and tmp.");
    Configuration conf = new Configuration();
    if (drillConfig.hasPath(ExecConstants.DFDL_DIRECTORY_FS)) {
      conf.set(FileSystem.FS_DEFAULT_NAME_KEY, drillConfig.getString(ExecConstants.DFDL_DIRECTORY_FS));
    }
    try {
      fs = FileSystem.get(conf);
    } catch (IOException e) {
      throw DrillRuntimeException.create(e,
          "Error during file system %s setup", conf.get(FileSystem.FS_DEFAULT_NAME_KEY));
    }

    if (drillConfig.hasPath(ExecConstants.DFDL_DIRECTORY_ROOT)) {
      root = drillConfig.getString(ExecConstants.DFDL_DIRECTORY_ROOT);
    } else {
      root = fs.getHomeDirectory().toUri().getPath();
    }

    registryArea = createArea(drillConfig.getString(ExecConstants.DFDL_DIRECTORY_REGISTRY));
    stagingArea = createArea(drillConfig.getString(ExecConstants.DFDL_DIRECTORY_STAGING));
    tmpArea = createArea(drillConfig.getString(ExecConstants.DFDL_DIRECTORY_TMP));
  }


  /**
   * Concatenates daffodil area with root directory.
   * Creates daffodil area, if area does not exist.
   * Checks if area exists and is a directory, if it is writable for current user,
   * throws {@link DrillRuntimeException} otherwise.
   *
   * @param directory directory path
   * @return path to area
   */
  private Path createArea(String directory) {
    Path path = new Path(new File(root, directory).toURI().getPath());
    String fullPath = path.toUri().getPath();
    try {
      fs.mkdirs(path);
      Preconditions.checkState(fs.exists(path), "Area [%s] must exist", fullPath);
      FileStatus fileStatus = fs.getFileStatus(path);
      Preconditions.checkState(fileStatus.isDirectory(), "Area [%s] must be a directory", fullPath);
      FsPermission permission = fileStatus.getPermission();
      // The process user has write rights on directory if:
      // 1. process user is owner of the directory and has write rights
      // 2. process user is in group that has write rights
      // 3. any user has write rights
      Preconditions.checkState(
          (ImpersonationUtil.getProcessUserName()
              .equals(fileStatus.getOwner())
              && permission.getUserAction().implies(FsAction.WRITE)) ||
              (Sets.newHashSet(ImpersonationUtil.getProcessUserGroupNames())
                  .contains(fileStatus.getGroup())
                  && permission.getGroupAction().implies(FsAction.WRITE)) ||
              permission.getOtherAction().implies(FsAction.WRITE),
          "Area [%s] must be writable and executable for application user", fullPath);
    } catch (Exception e) {
      if (e instanceof DrillRuntimeException) {
        throw (DrillRuntimeException) e;
      }
      throw DrillRuntimeException.create(e,
          "Error during daffodil area creation [%s] on file system [%s]", fullPath, fs.getUri());
    }
    logger.info("Created remote daffodil area [{}] on file system [{}]", fullPath, fs.getUri());
    return path;
  }

  /**
   * Checks if passed path exists on predefined file system.
   *
   * @param path path to be checked
   * @throws IOException if path does not exist
   */
  private void checkPathExistence(Path path) throws IOException {
    if (!fs.exists(path)) {
      throw new IOException(String.format("File %s does not exist on file system %s",
          path.toUri().getPath(), fs.getUri()));
    }
  }

  /**
   * Deletes quietly file or directory, in case of errors, logs warning and proceeds.
   *
   * @param path path to file or directory
   * @param isDirectory set to true if we need to delete a directory
   */
  private void deleteQuietly(Path path, boolean isDirectory) {
    try {
      fs.delete(path, isDirectory);
    } catch (IOException e) {
      logger.warn(String.format("Error during deletion [%s]", path.toUri().getPath()), e);
    }
  }


  /**
   * Registers Daffodil Schema dynamically. Process consists of several steps:
   * <ol>
   * <li>Registering jar in jar registry to ensure that several jars with the same name is not registered.</li>
   * <li>Binary and source jars validation and back up.</li>
   * <li>Validation against local function registry.</li>
   * <li>Validation against remote function registry.</li>
   * <li>Remote function registry update.</li>
   * <li>Copying of jars to registry area and clean up.</li>
   * </ol>
   *
   * UDFs registration is allowed only if dynamic UDFs support is enabled.
   *
   * @return - Single row indicating list of registered UDFs, or error message otherwise.
   */
  @Override
  public PhysicalPlan getPlan(SqlNode sqlNode) throws ForemanSetupException, IOException {

    //DFDLSchemaManager schemaManager = new DFDLSchemaManager(sqlNode);

    boolean inProgress = false;
  }
    /*try {
      final String action = remoteRegistry.addToJars(schemaManager.getBinaryName(), RemoteFunctionRegistry.Action.REGISTRATION);
      if (!(inProgress = action == null)) {
        return DirectPlan.createDirectPlan(context, false,
            String.format("Jar with %s name is used. Action: %s", schemaManager.getBinaryName(), action));
      }

      schemaManager.initRemoteBackup();
      initRemoteRegistration(functions, schemaManager, remoteRegistry);
      schemaManager.deleteQuietlyFromStagingArea();

      return DirectPlan.createDirectPlan(context, true,
          String.format("The following Daffodil schemata in jar %s have been registered:\n%s", schemaManager.getBinaryName(), functions));

    } catch (Exception e) {
      logger.error("Error during Daffodil schema registration", e);
      return DirectPlan.createDirectPlan(context, false, e.getMessage());
    } finally {
      if (inProgress) {
        remoteRegistry.removeFromJars(schemaManager.getBinaryName());
      }*/
      //schemaManager.cleanUp();

  /**
   * Instantiates remote registration. First gets remote function registry with version.
   * Version is used to ensure that we update the same registry we validated against.
   * Then validates against list of remote jars.
   * If validation is successful, first copies jars to registry area and starts updating remote function registry.
   * If during update {@link VersionMismatchException} was detected,
   * attempts to repeat remote registration process till retry attempts exceeds the limit.
   * If retry attempts number hits 0, throws exception that failed to update remote function registry.
   * In case of any error, if jars have been already copied to registry area, they will be deleted.
   *
   * @param functions list of functions present in jar
   * @param jarManager helper class for copying jars to registry area
   * @param remoteRegistry remote function registry
   * @throws IOException in case of problems with copying jars to registry area
   */
  /*private void initRemoteRegistration(List<String> functions,
      DFDLSchemaManager jarManager,
      RemoteFunctionRegistry remoteRegistry) throws IOException {
    int retryAttempts = remoteRegistry.getRetryAttempts();
    boolean copyJars = true;
    try {
      while (retryAttempts >= 0) {
        DataChangeVersion version = new DataChangeVersion();
        List<Jar> remoteJars = remoteRegistry.getRegistry(version).getJarList();
        validateAgainstRemoteRegistry(remoteJars, jarManager.getBinaryName(), functions);
        if (copyJars) {
          jarManager.copyToRegistryArea();
          copyJars = false;
        }
        List<Jar> jars = Lists.newArrayList(remoteJars);
        jars.add(Jar.newBuilder().setName(jarManager.getBinaryName()).addAllFunctionSignature(functions).build());
        Registry updatedRegistry = Registry.newBuilder().addAllJar(jars).build();
        try {
          remoteRegistry.updateRegistry(updatedRegistry, version);
          return;
        } catch (VersionMismatchException ex) {
          logger.debug("Failed to update function registry during registration, version mismatch was detected.", ex);
          retryAttempts--;
        }
      }
      throw new DrillRuntimeException("Failed to update remote function registry. Exceeded retry attempts limit.");
    } catch (Exception e) {
      if (!copyJars) {
        jarManager.deleteQuietlyFromRegistryArea();
      }
      throw e;
    }
  }*/



  /**
   * Inner helper class that encapsulates logic for working with DFDL Schemata Files.
   * During initialization it creates path to staging jar, local and remote temporary jars, registry jars.
   * Is responsible for validation, copying and deletion actions.
   */
  private class DFDLSchemaManager {

    private final String binaryName;
    private final FileSystem fs;

    private final Path remoteTmpDir;
    private final Path localTmpDir;

    private final Path stagingBinary;
    private final Path stagingSource;

    private final Path tmpRemoteBinary;
    private final Path tmpRemoteSource;

    private final Path registryBinary;
    private final Path registrySource;

    DFDLSchemaManager(SqlNode sqlNode) throws ForemanSetupException {
      SqlCreateDaffodilSchema node = unwrap(sqlNode, SqlCreateDaffodilSchema.class);
      this.binaryName = ((SqlCharStringLiteral) node.getJar()).toValue();
      String sourceName = JarUtil.getSourceName(binaryName);

      this.remoteTmpDir = new Path(remoteRegistry.getTmpArea(), UUID.randomUUID().toString());
      this.tmpRemoteBinary = new Path(remoteTmpDir, binaryName);
      this.tmpRemoteSource = new Path(remoteTmpDir, sourceName);

      this.stagingBinary = new Path(remoteRegistry.getStagingArea(), binaryName);
      this.stagingSource = new Path(remoteRegistry.getStagingArea(), sourceName);

      this.registryBinary = new Path(remoteRegistry.getRegistryArea(), binaryName);
      this.registrySource = new Path(remoteRegistry.getRegistryArea(), sourceName);

      this.localTmpDir = new Path(DrillFileUtils.createTempDir().toURI());
      this.fs = remoteRegistry.getFs();
    }

    /**
     * @return binary jar name
     */
    String getBinaryName() {
      return binaryName;
    }

    /**
     * Validates that both binary and source jar are present in staging area,
     * it is expected that binary and source have standard naming convention.
     * Backs up both jars to unique folder in remote temporary area.
     *
     * @throws IOException in case of binary or source absence or problems during copying jars
     */
    void initRemoteBackup() throws IOException {
      checkPathExistence(stagingBinary);
      checkPathExistence(stagingSource);
      fs.mkdirs(remoteTmpDir);
      FileUtil.copy(fs, stagingBinary, fs, tmpRemoteBinary, false, true, fs.getConf());
      FileUtil.copy(fs, stagingSource, fs, tmpRemoteSource, false, true, fs.getConf());
    }

    /**
     * Copies binary jar to unique folder on local file system.
     * Source jar is not needed for local validation.
     *
     * @return path to local binary jar
     * @throws IOException in case of problems during copying binary jar
     */
    Path copyBinaryToLocal() throws IOException {
      Path localBinary = new Path(localTmpDir, binaryName);
      fs.copyToLocalFile(tmpRemoteBinary, localBinary);
      return localBinary;
    }

    /**
     * Copies binary and source jars to registry area,
     * in case of {@link IOException} removes copied jar(-s) from registry area
     *
     * @throws IOException is re-thrown in case of problems during copying process
     */
    void copyToRegistryArea() throws IOException {
      FileUtil.copy(fs, tmpRemoteBinary, fs, registryBinary, false, true, fs.getConf());
      try {
        FileUtil.copy(fs, tmpRemoteSource, fs, registrySource, false, true, fs.getConf());
      } catch (IOException e) {
        deleteQuietly(registryBinary, false);
        throw new IOException(e);
      }
    }






  }
}
