/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.rpc.security;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;
import com.google.protobuf.Internal.EnumLite;
import com.google.protobuf.InvalidProtocolBufferException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import org.apache.drill.exec.proto.UserBitShared.SaslMessage;
import org.apache.drill.exec.proto.UserBitShared.SaslStatus;
import org.apache.drill.exec.rpc.RequestHandler;
import org.apache.drill.exec.rpc.Response;
import org.apache.drill.exec.rpc.ResponseSender;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.ServerConnection;
import org.apache.hadoop.security.UserGroupInformation;

import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;
import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.security.PrivilegedExceptionAction;
import java.util.EnumMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Handles SASL exchange, on the server-side.
 *
 * @param <S> Server connection type
 * @param <T> RPC type
 */
public class ServerAuthenticationHandler<S extends ServerConnection<S>, T extends EnumLite>
    implements RequestHandler<S> {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(ServerAuthenticationHandler.class);

  private static final ImmutableMap<SaslStatus, SaslResponseProcessor> RESPONSE_PROCESSORS;

  static {
    final Map<SaslStatus, SaslResponseProcessor> map = new EnumMap<>(SaslStatus.class);
    map.put(SaslStatus.SASL_START, new SaslStartProcessor());
    map.put(SaslStatus.SASL_IN_PROGRESS, new SaslInProgressProcessor());
    map.put(SaslStatus.SASL_SUCCESS, new SaslSuccessProcessor());
    map.put(SaslStatus.SASL_FAILED, new SaslFailedProcessor());
    RESPONSE_PROCESSORS = Maps.immutableEnumMap(map);
  }

  private final RequestHandler<S> requestHandler;
  private final int saslRequestTypeValue;
  private final T saslResponseType;

  public ServerAuthenticationHandler(final RequestHandler<S> requestHandler, final int saslRequestTypeValue,
                                     final T saslResponseType) {
    this.requestHandler = requestHandler;
    this.saslRequestTypeValue = saslRequestTypeValue;
    this.saslResponseType = saslResponseType;
  }

  @Override
  public void handle(S connection, int rpcType, ByteBuf pBody, ByteBuf dBody, ResponseSender sender)
      throws RpcException {
    final String remoteAddress = connection.getRemoteAddress().toString();

    // exchange involves server "challenges" and client "responses" (initiated by client)
    if (saslRequestTypeValue == rpcType) {
      final SaslMessage saslResponse;
      try {
        saslResponse = SaslMessage.PARSER.parseFrom(new ByteBufInputStream(pBody));
      } catch (final InvalidProtocolBufferException e) {
        handleAuthFailure(remoteAddress, sender, e, saslResponseType);
        return;
      }

      logger.trace("Received SASL message {} from {}", saslResponse.getStatus(), remoteAddress);
      final SaslResponseProcessor processor = RESPONSE_PROCESSORS.get(saslResponse.getStatus());
      if (processor == null) {
        logger.info("Unknown message type from client from {}. Will stop authentication.", remoteAddress);
        handleAuthFailure(remoteAddress, sender, new SaslException("Received unexpected message"),
            saslResponseType);
        return;
      }

      final SaslResponseContext<S, T> context = new SaslResponseContext<>(saslResponse, connection, remoteAddress,
          sender, requestHandler, saslResponseType);
      try {
        processor.process(context);
      } catch (final Exception e) {
        handleAuthFailure(remoteAddress, sender, e, saslResponseType);
      }
    } else {

      // this handler only handles messages of SASL_MESSAGE_VALUE type

      // the response type for this request type is likely known from UserRpcConfig,
      // but the client should not be making any requests before authenticating.
      // drop connection
      throw new RpcException(
          String.format("Request of type %d is not allowed without authentication. " +
                  "Client on %s must authenticate before making requests. Connection dropped.",
              rpcType, remoteAddress));
    }
  }

  private static class SaslResponseContext<S extends ServerConnection<S>, T extends EnumLite> {

    final SaslMessage saslResponse;
    final S connection;
    final String remoteAddress;
    final ResponseSender sender;
    final RequestHandler<S> requestHandler;
    final T saslResponseType;

    SaslResponseContext(SaslMessage saslResponse, S connection, String remoteAddress, ResponseSender sender,
                        RequestHandler<S> requestHandler, T saslResponseType) {
      this.saslResponse = checkNotNull(saslResponse);
      this.connection = checkNotNull(connection);
      this.remoteAddress = checkNotNull(remoteAddress);
      this.sender = checkNotNull(sender);
      this.requestHandler = checkNotNull(requestHandler);
      this.saslResponseType = checkNotNull(saslResponseType);
    }
  }

  private interface SaslResponseProcessor {

    /**
     * Process response from client, and if there are no exceptions, send response using
     * {@link SaslResponseContext#sender}. Otherwise, throw the exception.
     *
     * @param context response context
     */
    <S extends ServerConnection<S>, T extends EnumLite>
    void process(SaslResponseContext<S, T> context) throws Exception;

  }

  private static class SaslStartProcessor implements SaslResponseProcessor {

    @Override
    public <S extends ServerConnection<S>, T extends EnumLite>
    void process(SaslResponseContext<S, T> context) throws Exception {
      context.connection.initSaslServer(context.saslResponse.getMechanism());

      // assume #evaluateResponse must be called at least once
      RESPONSE_PROCESSORS.get(SaslStatus.SASL_IN_PROGRESS).process(context);
    }
  }

  private static class SaslInProgressProcessor implements SaslResponseProcessor {

    @Override
    public <S extends ServerConnection<S>, T extends EnumLite>
    void process(SaslResponseContext<S, T> context) throws Exception {
      final SaslMessage.Builder challenge = SaslMessage.newBuilder();
      final SaslServer saslServer = context.connection.getSaslServer();

      final byte[] challengeBytes = evaluateResponse(saslServer, context.saslResponse.getData().toByteArray());

      if (saslServer.isComplete()) {
        challenge.setStatus(SaslStatus.SASL_SUCCESS);
        if (challengeBytes != null) {
          challenge.setData(ByteString.copyFrom(challengeBytes));
        }

        handleSuccess(context, challenge, saslServer);
      } else {
        challenge.setStatus(SaslStatus.SASL_IN_PROGRESS)
            .setData(ByteString.copyFrom(challengeBytes));
        context.sender.send(new Response(context.saslResponseType, challenge.build()));
      }
    }
  }

  // only when client succeeds first
  private static class SaslSuccessProcessor implements SaslResponseProcessor {

    @Override
    public <S extends ServerConnection<S>, T extends EnumLite>
    void process(SaslResponseContext<S, T> context) throws Exception {
      // at this point, #isComplete must be false; so try once, fail otherwise
      final SaslServer saslServer = context.connection.getSaslServer();

      evaluateResponse(saslServer, context.saslResponse.getData().toByteArray()); // discard challenge

      if (saslServer.isComplete()) {
        final SaslMessage.Builder challenge = SaslMessage.newBuilder();
        challenge.setStatus(SaslStatus.SASL_SUCCESS);

        handleSuccess(context, challenge, saslServer);
      } else {
        logger.info("Failed to authenticate client from {}", context.remoteAddress);
        throw new SaslException("Client allegedly succeeded authentication, but server did not. Suspicious?");
      }
    }
  }

  private static class SaslFailedProcessor implements SaslResponseProcessor {

    @Override
    public <S extends ServerConnection<S>, T extends EnumLite>
    void process(SaslResponseContext<S, T> context) throws Exception {
      logger.info("Client from {} failed authentication graciously, and does not want to continue.",
          context.remoteAddress);
      throw new SaslException("Client graciously failed authentication");
    }
  }

  private static byte[] evaluateResponse(final SaslServer saslServer,
                                         final byte[] responseBytes) throws SaslException {
    try {
      return UserGroupInformation.getLoginUser()
          .doAs(new PrivilegedExceptionAction<byte[]>() {
            @Override
            public byte[] run() throws Exception {
              return saslServer.evaluateResponse(responseBytes);
            }
          });
    } catch (final UndeclaredThrowableException e) {
      throw new SaslException(String.format("Unexpected failure trying to authenticate using %s",
          saslServer.getMechanismName()), e.getCause());
    } catch (final IOException | InterruptedException e) {
      if (e instanceof SaslException) {
        throw (SaslException) e;
      } else {
        throw new SaslException(String.format("Unexpected failure trying to authenticate using %s",
            saslServer.getMechanismName()), e);
      }
    }
  }

  private static <S extends ServerConnection<S>, T extends EnumLite>
  void handleSuccess(final SaslResponseContext<S, T> context, final SaslMessage.Builder challenge,
                     final SaslServer saslServer) throws IOException {
    context.connection.changeHandlerTo(context.requestHandler);
    context.connection.finalizeSaslSession();
    context.sender.send(new Response(context.saslResponseType, challenge.build()));

    // setup security layers here..

    if (logger.isTraceEnabled()) {
      logger.trace("Authenticated {} successfully using {} from {}", saslServer.getAuthorizationID(),
          saslServer.getMechanismName(), context.remoteAddress);
    }
  }

  private static final SaslMessage SASL_FAILED_MESSAGE =
      SaslMessage.newBuilder().setStatus(SaslStatus.SASL_FAILED).build();

  private static <T extends EnumLite>
  void handleAuthFailure(final String remoteAddress, final ResponseSender sender,
                         final Exception e, final T saslResponseType) throws RpcException {
    logger.debug("Authentication failed from client {} due to {}", remoteAddress, e);

    // inform the client that authentication failed, and no more
    sender.send(new Response(saslResponseType, SASL_FAILED_MESSAGE));

    // drop connection
    throw new RpcException(e);
  }
}
