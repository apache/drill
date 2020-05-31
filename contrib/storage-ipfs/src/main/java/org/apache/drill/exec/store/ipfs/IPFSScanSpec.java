/*
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


package org.apache.drill.exec.store.ipfs;


import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.ipfs.multihash.Multihash;
import org.apache.drill.common.PlanStringBuilder;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.InvalidParameterException;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


@JsonTypeName("IPFSScanSpec")
public class IPFSScanSpec {
  private static final Logger logger = LoggerFactory.getLogger(IPFSScanSpec.class);

  public enum Prefix {
    @JsonProperty("ipfs")
    IPFS("ipfs"),
    @JsonProperty("ipns")
    IPNS("ipns");

    @JsonProperty("prefix")
    private final String name;

    Prefix(String prefix) {
      this.name = prefix;
    }

    @Override
    public String toString() {
      return this.name;
    }

    @JsonCreator
    public static Prefix of(String what) {
      switch (what) {
        case "ipfs":
          return IPFS;
        case "ipns":
          return IPNS;
        default:
          throw new InvalidParameterException("Unsupported prefix: " + what);
      }
    }
  }

  public enum Format {
    @JsonProperty("json")
    JSON("json"),
    @JsonProperty("csv")
    CSV("csv");

    @JsonProperty("format")
    private final String name;

    Format(String prefix) {
      this.name = prefix;
    }

    @Override
    public String toString() {
      return this.name;
    }

    @JsonCreator
    public static Format of(String what) {
      switch (what) {
        case "json":
          return JSON;
        case "csv":
          return CSV;
        default:
          throw new InvalidParameterException("Unsupported format: " + what);
      }
    }
  }

  public static Set<String> formats = ImmutableSet.of("json", "csv");
  private Prefix prefix;
  private String path;
  private Format formatExtension;
  private final IPFSContext ipfsContext;

  @JsonCreator
  public IPFSScanSpec(@JacksonInject StoragePluginRegistry registry,
                      @JsonProperty("IPFSStoragePluginConfig") IPFSStoragePluginConfig ipfsStoragePluginConfig,
                      @JsonProperty("prefix") Prefix prefix,
                      @JsonProperty("format") Format format,
                      @JsonProperty("path") String path) {
    this.ipfsContext = registry.resolve(ipfsStoragePluginConfig, IPFSStoragePlugin.class).getIPFSContext();
    this.prefix = prefix;
    this.formatExtension = format;
    this.path = path;
  }

  public IPFSScanSpec(IPFSContext ipfsContext, String path) {
    this.ipfsContext = ipfsContext;
    parsePath(path);
  }

  private void parsePath(String path) {
    // IPFS CIDs can be encoded in various bases, see https://github.com/multiformats/multibase/blob/master/multibase.csv
    // Base64-encoded CIDs should not be present in a path since it can contain the '/' character.
    // [a-zA-Z0-9] should be enough to cover the other bases.
    Pattern tableNamePattern = Pattern.compile("^/(ipfs|ipns)/([a-zA-Z0-9]+(/[^#]+)*)(?:#(\\w+))?$");
    Matcher matcher = tableNamePattern.matcher(path);
    if (!matcher.matches()) {
      throw UserException
          .validationError()
          .message("Invalid IPFS path in query string. Use paths of pattern " +
              "`/scheme/hashpath#format`, where scheme:= \"ipfs\"|\"ipns\", " +
              "hashpath:= HASH [\"/\" path], HASH is IPFS Base58 encoded hash, " +
              "path:= TEXT [\"/\" path], format:= \"json\"|\"csv\"")
          .build(logger);
    }

    String prefix = matcher.group(1);
    String hashPath = matcher.group(2);
    String formatExtension = matcher.group(4);
    if (formatExtension == null) {
      formatExtension = "_FORMAT_OMITTED_";
    }

    logger.debug("prefix {}, hashPath {}, format {}", prefix, hashPath, formatExtension);

    this.path = hashPath;
    this.prefix = Prefix.of(prefix);
    try {
      this.formatExtension = Format.of(formatExtension);
    } catch (InvalidParameterException e) {
      //if format is omitted or not valid, try resolve it from file extension in the path
      Pattern fileExtensionPattern = Pattern.compile("^.*\\.(\\w+)$");
      Matcher fileExtensionMatcher = fileExtensionPattern.matcher(hashPath);
      if (fileExtensionMatcher.matches()) {
        this.formatExtension = Format.of(fileExtensionMatcher.group(1));
        logger.debug("extracted format from query: {}", this.formatExtension);
      } else {
        logger.debug("failed to extract format from path: {}", hashPath);
        throw UserException
            .validationError()
            .message("File format is missing and cannot be extracted from query: %s. " +
                "Please specify file format explicitly by appending `#csv` or `#json`, etc, to the IPFS path.", hashPath)
            .build(logger);
      }
    }
  }

  /**
   * Resolve target hash from IPFS/IPNS paths.
   * e.g. /ipfs/hash/path/file will be resolved to /ipfs/file_hash
   *
   * @param helper IPFS helper
   * @return the resolved target hash
   */
  @JsonProperty
  public Multihash getTargetHash(IPFSHelper helper) {
    try {
      Multihash topHash = helper.resolve(prefix.toString(), path, true);
      if (topHash == null) {
        throw UserException.validationError().message("Non-existent IPFS path: %s", toString()).build(logger);
      }
      return topHash;
    } catch (Exception e) {
      throw UserException
          .executionError(e)
          .message("Unable to resolve IPFS path; is it a valid IPFS path?")
          .build(logger);
    }
  }

  @JsonProperty
  public Prefix getPrefix() {
    return prefix;
  }

  @JsonProperty
  public String getPath() {
    return path;
  }

  @JsonProperty
  public Format getFormatExtension() {
    return formatExtension;
  }

  @JsonIgnore
  public IPFSContext getIPFSContext() {
    return ipfsContext;
  }

  @JsonProperty("IPFSStoragePluginConfig")
  public IPFSStoragePluginConfig getIPFSStoragePluginConfig() {
    return ipfsContext.getStoragePluginConfig();
  }

  @Override
  public String toString() {
    return new PlanStringBuilder(this)
        .field("prefix", prefix)
        .field("path", path)
        .field("format", formatExtension)
        .toString();
  }
}
