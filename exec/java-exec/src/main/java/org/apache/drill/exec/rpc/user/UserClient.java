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
package org.apache.drill.exec.rpc.user;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;

import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;

import org.apache.drill.common.KerberosUtil;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.config.DrillProperties;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.proto.GeneralRPCProtos.Ack;
import org.apache.drill.exec.proto.UserBitShared.QueryData;
import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.proto.UserBitShared.QueryResult;
import org.apache.drill.exec.proto.UserBitShared.SaslMessage;
import org.apache.drill.exec.proto.UserBitShared.UserCredentials;
import org.apache.drill.exec.proto.UserProtos.BitToUserHandshake;
import org.apache.drill.exec.proto.UserProtos.CreatePreparedStatementResp;
import org.apache.drill.exec.proto.UserProtos.GetCatalogsResp;
import org.apache.drill.exec.proto.UserProtos.GetColumnsResp;
import org.apache.drill.exec.proto.UserProtos.GetQueryPlanFragments;
import org.apache.drill.exec.proto.UserProtos.GetSchemasResp;
import org.apache.drill.exec.proto.UserProtos.GetServerMetaResp;
import org.apache.drill.exec.proto.UserProtos.GetTablesResp;
import org.apache.drill.exec.proto.UserProtos.QueryPlanFragments;
import org.apache.drill.exec.proto.UserProtos.RpcEndpointInfos;
import org.apache.drill.exec.proto.UserProtos.RpcType;
import org.apache.drill.exec.proto.UserProtos.RunQuery;
import org.apache.drill.exec.proto.UserProtos.SaslSupport;
import org.apache.drill.exec.proto.UserProtos.UserToBitHandshake;
import org.apache.drill.exec.rpc.AbstractClientConnection;
import org.apache.drill.exec.rpc.Acks;
import org.apache.drill.exec.rpc.BasicClient;
import org.apache.drill.exec.rpc.DrillRpcFuture;
import org.apache.drill.exec.rpc.NonTransientRpcException;
import org.apache.drill.exec.rpc.OutOfMemoryHandler;
import org.apache.drill.exec.rpc.ProtobufLengthDecoder;
import org.apache.drill.exec.rpc.Response;
import org.apache.drill.exec.rpc.ResponseSender;
import org.apache.drill.exec.rpc.RpcConnectionHandler;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.RpcOutcomeListener;
import org.apache.drill.exec.rpc.security.AuthStringUtil;
import org.apache.drill.exec.rpc.security.AuthenticationOutcomeListener;
import org.apache.drill.exec.rpc.security.AuthenticatorFactory;
import org.apache.drill.exec.rpc.security.ClientAuthenticatorProvider;
import org.apache.drill.exec.rpc.security.plain.PlainFactory;
import org.apache.drill.exec.rpc.security.SaslProperties;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;

import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AbstractCheckedFuture;
import com.google.common.util.concurrent.CheckedFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.MessageLite;


import io.netty.buffer.ByteBuf;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;

public class UserClient extends BasicClient<RpcType, UserClient.UserToBitConnection,
    UserToBitHandshake, BitToUserHandshake> {
  private static final Logger logger = org.slf4j.LoggerFactory.getLogger(UserClient.class);

  private final BufferAllocator allocator;
  private final QueryResultHandler queryResultHandler = new QueryResultHandler();
  private final String clientName;
  private final boolean supportComplexTypes;

  private RpcEndpointInfos serverInfos = null;
  private Set<RpcType> supportedMethods = null;

  // these are used for authentication
  private volatile List<String> serverAuthMechanisms = null;
  private volatile boolean authComplete = true;

  public UserClient(String clientName, DrillConfig config, boolean supportComplexTypes,
      BufferAllocator allocator, EventLoopGroup eventLoopGroup, Executor eventExecutor) {
    super(
        UserRpcConfig.getMapping(config, eventExecutor),
        allocator.getAsByteBufAllocator(),
        eventLoopGroup,
        RpcType.HANDSHAKE,
        BitToUserHandshake.class,
        BitToUserHandshake.PARSER);
    this.clientName = clientName;
    this.allocator = allocator;
    this.supportComplexTypes = supportComplexTypes;
  }

  public RpcEndpointInfos getServerInfos() {
    return serverInfos;
  }

  public Set<RpcType> getSupportedMethods() {
    return supportedMethods;
  }

  public void submitQuery(UserResultsListener resultsListener, RunQuery query) {
    send(queryResultHandler.getWrappedListener(resultsListener), RpcType.RUN_QUERY, query, QueryId.class);
  }

  /**
   * Connects, and if required, authenticates. This method blocks until both operations are complete.
   *
   * @param endpoint endpoint to connect to
   * @param properties properties
   * @param credentials credentials
   * @throws RpcException if either connection or authentication fails
   */
  public void connect(final DrillbitEndpoint endpoint, final DrillProperties properties,
                      final UserCredentials credentials) throws RpcException {
    final UserToBitHandshake.Builder hsBuilder = UserToBitHandshake.newBuilder()
        .setRpcVersion(UserRpcConfig.RPC_VERSION)
        .setSupportListening(true)
        .setSupportComplexTypes(supportComplexTypes)
        .setSupportTimeout(true)
        .setCredentials(credentials)
        .setClientInfos(UserRpcUtils.getRpcEndpointInfos(clientName))
        .setSaslSupport(SaslSupport.SASL_PRIVACY)
        .setProperties(properties.serializeForServer());

    // Only used for testing purpose
    if (properties.containsKey(DrillProperties.TEST_SASL_LEVEL)) {
      hsBuilder.setSaslSupport(SaslSupport.valueOf(
          Integer.parseInt(properties.getProperty(DrillProperties.TEST_SASL_LEVEL))));
    }

    connect(hsBuilder.build(), endpoint).checkedGet();

    // Check if client needs encryption and server is not configured for encryption.
    final boolean clientNeedsEncryption = properties.containsKey(DrillProperties.SASL_ENCRYPT)
        && Boolean.parseBoolean(properties.getProperty(DrillProperties.SASL_ENCRYPT));

    if(clientNeedsEncryption && !connection.isEncryptionEnabled()) {
      throw new NonTransientRpcException("Client needs encrypted connection but server is not configured for " +
          "encryption. Please check connection parameter or contact your administrator");
    }

    if (serverAuthMechanisms != null) {
      try {
        authenticate(properties).checkedGet();
      } catch (final SaslException e) {
        throw new NonTransientRpcException(e);
      }
    }
  }

  private CheckedFuture<Void, RpcException> connect(final UserToBitHandshake handshake,
                                                    final DrillbitEndpoint endpoint) {
    final SettableFuture<Void> connectionSettable = SettableFuture.create();
    final CheckedFuture<Void, RpcException> connectionFuture =
        new AbstractCheckedFuture<Void, RpcException>(connectionSettable) {
          @Override
          protected RpcException mapException(Exception e) {
            return RpcException.mapException(e);
          }
        };
    final RpcConnectionHandler<UserToBitConnection> connectionHandler =
        new RpcConnectionHandler<UserToBitConnection>() {
          @Override
          public void connectionSucceeded(UserToBitConnection connection) {
            connectionSettable.set(null);
          }

          @Override
          public void connectionFailed(FailureType type, Throwable t) {
            connectionSettable.setException(new RpcException(String.format("%s : %s",
                type.name(), t.getMessage()), t));
          }
        };

    connectAsClient(queryResultHandler.getWrappedConnectionHandler(connectionHandler),
        handshake, endpoint.getAddress(), endpoint.getUserPort());

    return connectionFuture;
  }

  private CheckedFuture<Void, SaslException> authenticate(final DrillProperties properties) {
    final Map<String, String> propertiesMap = properties.stringPropertiesAsMap();

    // Set correct QOP property and Strength based on server needs encryption or not.
    // If ChunkMode is enabled then negotiate for buffer size equal to wrapChunkSize,
    // If ChunkMode is disabled then negotiate for MAX_WRAPPED_SIZE buffer size.
    propertiesMap.putAll(SaslProperties.getSaslProperties(connection.isEncryptionEnabled(),
                                                          connection.getMaxWrappedSize()));

    final SettableFuture<Void> authSettable = SettableFuture.create(); // use handleAuthFailure to setException
    final CheckedFuture<Void, SaslException> authFuture =
        new AbstractCheckedFuture<Void, SaslException>(authSettable) {

          @Override
          protected SaslException mapException(Exception e) {
            if (e instanceof ExecutionException) {
              final Throwable cause = Throwables.getRootCause(e);
              if (cause instanceof SaslException) {
                return new SaslException(String.format("Authentication failed. [Details: %s, Error %s]",
                    connection.getEncryptionCtxtString(), cause.getMessage()), cause);
              }
            }
            return new SaslException(String.format("Authentication failed unexpectedly. [Details: %s, Error %s]",
                connection.getEncryptionCtxtString(), e.getMessage()), e);
          }
        };

    final AuthenticatorFactory factory;
    final String mechanismName;
    final UserGroupInformation ugi;
    final SaslClient saslClient;
    try {
      factory = getAuthenticatorFactory(properties);
      mechanismName = factory.getSimpleName();
      logger.trace("Will try to authenticate to server using {} mechanism with encryption context {}",
          mechanismName, connection.getEncryptionCtxtString());
      ugi = factory.createAndLoginUser(propertiesMap);
      saslClient = factory.createSaslClient(ugi, propertiesMap);
      if (saslClient == null) {
        throw new SaslException(String.format("Cannot initiate authentication using %s mechanism. Insufficient " +
            "credentials or selected mechanism doesn't support configured security layers?", factory.getSimpleName()));
      }
      connection.setSaslClient(saslClient);
    } catch (final IOException e) {
      authSettable.setException(e);
      return authFuture;
    }

    logger.trace("Initiating SASL exchange.");
    new AuthenticationOutcomeListener<>(this, connection, RpcType.SASL_MESSAGE, ugi,
        new RpcOutcomeListener<Void>() {
          @Override
          public void failed(RpcException ex) {
            authSettable.setException(ex);
          }

          @Override
          public void success(Void value, ByteBuf buffer) {
            authComplete = true;
            authSettable.set(null);
          }

          @Override
          public void interrupted(InterruptedException e) {
            authSettable.setException(e);
          }
        }).initiate(mechanismName);
    return authFuture;
  }

  private AuthenticatorFactory getAuthenticatorFactory(final DrillProperties properties) throws SaslException {
    final Set<String> mechanismSet = AuthStringUtil.asSet(serverAuthMechanisms);

    // first, check if a certain mechanism must be used
    String authMechanism = properties.getProperty(DrillProperties.AUTH_MECHANISM);
    if (authMechanism != null) {
      if (!ClientAuthenticatorProvider.getInstance().containsFactory(authMechanism)) {
        throw new SaslException(String.format("Unknown mechanism: %s", authMechanism));
      }
      if (!mechanismSet.contains(authMechanism.toUpperCase())) {
        throw new SaslException(String.format("Server does not support authentication using: %s. [Details: %s]",
            authMechanism, connection.getEncryptionCtxtString()));
      }
      return ClientAuthenticatorProvider.getInstance()
          .getAuthenticatorFactory(authMechanism);
    }

    // check if Kerberos is supported, and the service principal is provided
    if (mechanismSet.contains(KerberosUtil.KERBEROS_SIMPLE_NAME) &&
        properties.containsKey(DrillProperties.SERVICE_PRINCIPAL)) {
      return ClientAuthenticatorProvider.getInstance()
          .getAuthenticatorFactory(KerberosUtil.KERBEROS_SIMPLE_NAME);
    }

    // check if username/password is supported, and username/password are provided
    if (mechanismSet.contains(PlainFactory.SIMPLE_NAME) &&
        properties.containsKey(DrillProperties.USER) &&
        !Strings.isNullOrEmpty(properties.getProperty(DrillProperties.PASSWORD))) {
      return ClientAuthenticatorProvider.getInstance()
          .getAuthenticatorFactory(PlainFactory.SIMPLE_NAME);
    }

    throw new SaslException(String.format("Server requires authentication using %s. Insufficient credentials?. " +
        "[Details: %s]. ", serverAuthMechanisms, connection.getEncryptionCtxtString()));
  }

  protected <SEND extends MessageLite, RECEIVE extends MessageLite>
  void send(RpcOutcomeListener<RECEIVE> listener, RpcType rpcType, SEND protobufBody, Class<RECEIVE> clazz,
            boolean allowInEventLoop, ByteBuf... dataBodies) {
    super.send(listener, connection, rpcType, protobufBody, clazz, allowInEventLoop, dataBodies);
  }

  @Override
  protected MessageLite getResponseDefaultInstance(int rpcType) throws RpcException {
    switch (rpcType) {
    case RpcType.ACK_VALUE:
      return Ack.getDefaultInstance();
    case RpcType.HANDSHAKE_VALUE:
      return BitToUserHandshake.getDefaultInstance();
    case RpcType.QUERY_HANDLE_VALUE:
      return QueryId.getDefaultInstance();
    case RpcType.QUERY_RESULT_VALUE:
      return QueryResult.getDefaultInstance();
    case RpcType.QUERY_DATA_VALUE:
      return QueryData.getDefaultInstance();
    case RpcType.QUERY_PLAN_FRAGMENTS_VALUE:
      return QueryPlanFragments.getDefaultInstance();
    case RpcType.CATALOGS_VALUE:
      return GetCatalogsResp.getDefaultInstance();
    case RpcType.SCHEMAS_VALUE:
      return GetSchemasResp.getDefaultInstance();
    case RpcType.TABLES_VALUE:
      return GetTablesResp.getDefaultInstance();
    case RpcType.COLUMNS_VALUE:
      return GetColumnsResp.getDefaultInstance();
    case RpcType.PREPARED_STATEMENT_VALUE:
      return CreatePreparedStatementResp.getDefaultInstance();
    case RpcType.SASL_MESSAGE_VALUE:
      return SaslMessage.getDefaultInstance();
    case RpcType.SERVER_META_VALUE:
      return GetServerMetaResp.getDefaultInstance();
    }
    throw new RpcException(String.format("Unable to deal with RpcType of %d", rpcType));
  }

  @Override
  protected void handle(UserToBitConnection connection, int rpcType, ByteBuf pBody, ByteBuf dBody,
                        ResponseSender sender) throws RpcException {
    if (!authComplete) {
      // Remote should not be making any requests before authenticating, drop connection
      throw new RpcException(String.format("Request of type %d is not allowed without authentication. " +
          "Remote on %s must authenticate before making requests. Connection dropped.",
          rpcType, connection.getRemoteAddress()));
    }
    switch (rpcType) {
    case RpcType.QUERY_DATA_VALUE:
      queryResultHandler.batchArrived(connection, pBody, dBody);
      sender.send(new Response(RpcType.ACK, Acks.OK));
      break;
    case RpcType.QUERY_RESULT_VALUE:
      queryResultHandler.resultArrived(pBody);
      sender.send(new Response(RpcType.ACK, Acks.OK));
      break;
    default:
      throw new RpcException(String.format("Unknown Rpc Type %d. ", rpcType));
    }
  }

  @Override
  protected void validateHandshake(BitToUserHandshake inbound) throws RpcException {
//    logger.debug("Handling handshake from bit to user. {}", inbound);
    if (inbound.hasServerInfos()) {
      serverInfos = inbound.getServerInfos();
    }
    supportedMethods = Sets.immutableEnumSet(inbound.getSupportedMethodsList());

    switch (inbound.getStatus()) {
    case SUCCESS:
      break;
    case AUTH_REQUIRED: {
      authComplete = false;
      serverAuthMechanisms = ImmutableList.copyOf(inbound.getAuthenticationMechanismsList());
      connection.setEncryption(inbound.hasEncrypted() && inbound.getEncrypted());

      if (inbound.hasMaxWrappedSize()) {
        connection.setMaxWrappedSize(inbound.getMaxWrappedSize());
      }
      logger.trace(String.format("Server requires authentication with encryption context %s before proceeding.",
          connection.getEncryptionCtxtString()));
      break;
    }
    case AUTH_FAILED:
    case RPC_VERSION_MISMATCH:
    case UNKNOWN_FAILURE:
      final String errMsg = String.format("Status: %s, Error Id: %s, Error message: %s",
          inbound.getStatus(), inbound.getErrorId(), inbound.getErrorMessage());
      logger.error(errMsg);
      throw new NonTransientRpcException(errMsg);
    }
  }

  @Override
  protected UserToBitConnection initRemoteConnection(SocketChannel channel) {
    super.initRemoteConnection(channel);
    return new UserToBitConnection(channel);
  }

  public class UserToBitConnection extends AbstractClientConnection {

    UserToBitConnection(SocketChannel channel) {

      // by default connection is not set for encryption. After receiving handshake msg from server we set the
      // isEncryptionEnabled, useChunkMode and chunkModeSize correctly.
      super(channel, "user client");
    }

    @Override
    public BufferAllocator getAllocator() {
      return allocator;
    }

    @Override
    protected Logger getLogger() {
      return logger;
    }

    @Override
    public void incConnectionCounter() {
      // no-op
    }

    @Override
    public void decConnectionCounter() {
      // no-op
    }
  }

  @Override
  public ProtobufLengthDecoder getDecoder(BufferAllocator allocator) {
    return new UserProtobufLengthDecoder(allocator, OutOfMemoryHandler.DEFAULT_INSTANCE);
  }

  /**
   * planQuery is an API to plan a query without query execution
   * @param req - data necessary to plan query
   * @return list of PlanFragments that can later on be submitted for execution
   */
  public DrillRpcFuture<QueryPlanFragments> planQuery(
      GetQueryPlanFragments req) {
    return send(RpcType.GET_QUERY_PLAN_FRAGMENTS, req, QueryPlanFragments.class);
  }
}
