/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
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
import com.google.protobuf.MessageLite;
import io.netty.buffer.ByteBuf;
import org.apache.drill.exec.proto.UserBitShared.SaslMessage;
import org.apache.drill.exec.proto.UserBitShared.SaslStatus;
import org.apache.drill.exec.rpc.BasicClient;
import org.apache.drill.exec.rpc.ClientConnection;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.RpcOutcomeListener;
import org.apache.hadoop.security.UserGroupInformation;

import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;
import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.security.PrivilegedExceptionAction;
import java.util.EnumMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Handles SASL exchange, on the client-side.
 *
 * @param <T> handshake rpc type
 * @param <C> Client connection type
 * @param <HS> Handshake send type
 * @param <HR> Handshake receive type
 */
public class AuthenticationOutcomeListener<T extends EnumLite, C extends ClientConnection,
    HS extends MessageLite, HR extends MessageLite>
    implements RpcOutcomeListener<SaslMessage> {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(AuthenticationOutcomeListener.class);

  private static final ImmutableMap<SaslStatus, SaslChallengeProcessor> CHALLENGE_PROCESSORS;
  static {
    final Map<SaslStatus, SaslChallengeProcessor> map = new EnumMap<>(SaslStatus.class);
    map.put(SaslStatus.SASL_IN_PROGRESS, new SaslInProgressProcessor());
    map.put(SaslStatus.SASL_SUCCESS, new SaslSuccessProcessor());
    map.put(SaslStatus.SASL_FAILED, new SaslFailedProcessor());
    CHALLENGE_PROCESSORS = Maps.immutableEnumMap(map);
  }

  private final BasicClient<T, C, HS, HR> client;
  private final C connection;
  private final T saslRpcType;
  private final UserGroupInformation ugi;
  private final RpcOutcomeListener<?> completionListener;

  public AuthenticationOutcomeListener(BasicClient<T, C, HS, HR> client,
                                       C connection, T saslRpcType, UserGroupInformation ugi,
                                       RpcOutcomeListener<?> completionListener) {
    this.client = client;
    this.connection = connection;
    this.saslRpcType = saslRpcType;
    this.ugi = ugi;
    this.completionListener = completionListener;
  }

  public void initiate(final String mechanismName) {
    logger.trace("Initiating SASL exchange.");
    try {
      final ByteString responseData;
      final SaslClient saslClient = connection.getSaslClient();
      if (saslClient.hasInitialResponse()) {
        responseData = ByteString.copyFrom(evaluateChallenge(ugi, saslClient, new byte[0]));
      } else {
        responseData = ByteString.EMPTY;
      }
      client.send(new AuthenticationOutcomeListener<>(client, connection, saslRpcType, ugi, completionListener),
          connection,
          saslRpcType,
          SaslMessage.newBuilder()
              .setMechanism(mechanismName)
              .setStatus(SaslStatus.SASL_START)
              .setData(responseData)
              .build(),
          SaslMessage.class,
          true /** the connection will not be backed up at this point */);
      logger.trace("Initiated SASL exchange.");
    } catch (final Exception e) {
      completionListener.failed(RpcException.mapException(e));
    }
  }

  @Override
  public void failed(RpcException ex) {
    completionListener.failed(RpcException.mapException(ex));
  }

  @Override
  public void success(SaslMessage value, ByteBuf buffer) {
    logger.trace("Server responded with message of type: {}", value.getStatus());
    final SaslChallengeProcessor processor = CHALLENGE_PROCESSORS.get(value.getStatus());
    if (processor == null) {
      completionListener.failed(RpcException.mapException(
          new SaslException("Server sent a corrupt message.")));
    } else {
      try {
        final SaslChallengeContext context = new SaslChallengeContext(value, connection.getSaslClient(), ugi);

        final SaslMessage saslResponse = processor.process(context);

        if (saslResponse != null) {
          client.send(new AuthenticationOutcomeListener<>(client, connection, saslRpcType, ugi, completionListener),
              connection, saslRpcType, saslResponse, SaslMessage.class,
              true /** the connection will not be backed up at this point */);
        } else {
          // success
          completionListener.success(null, null);
        }
      } catch (final Exception e) {
        completionListener.failed(RpcException.mapException(e));
      }
    }
  }

  @Override
  public void interrupted(InterruptedException e) {
    completionListener.interrupted(e);
  }

  private static class SaslChallengeContext {

    final SaslMessage challenge;
    final SaslClient saslClient;
    final UserGroupInformation ugi;

    SaslChallengeContext(SaslMessage challenge, SaslClient saslClient, UserGroupInformation ugi) {
      this.challenge = checkNotNull(challenge);
      this.saslClient = checkNotNull(saslClient);
      this.ugi = checkNotNull(ugi);
    }
  }

  private interface SaslChallengeProcessor {

    /**
     * Process challenge from server, and return a response.
     *
     * Returns null iff SASL exchange is complete and successful.
     *
     * @param context challenge context
     * @return response
     * @throws Exception
     */
    SaslMessage process(SaslChallengeContext context) throws Exception;

  }

  private static class SaslInProgressProcessor implements SaslChallengeProcessor {

    @Override
    public SaslMessage process(SaslChallengeContext context) throws Exception {
      final SaslMessage.Builder response = SaslMessage.newBuilder();

      final byte[] responseBytes = evaluateChallenge(context.ugi, context.saslClient,
          context.challenge.getData().toByteArray());

      final boolean isComplete = context.saslClient.isComplete();
      logger.trace("Evaluated challenge. Completed? {}.", isComplete);
      response.setData(responseBytes != null ? ByteString.copyFrom(responseBytes) : ByteString.EMPTY);
      // if isComplete, the client will get one more response from server
      response.setStatus(isComplete ? SaslStatus.SASL_SUCCESS : SaslStatus.SASL_IN_PROGRESS);
      return response.build();
    }
  }

  private static class SaslSuccessProcessor implements SaslChallengeProcessor {

    @Override
    public SaslMessage process(SaslChallengeContext context) throws Exception {
      if (context.saslClient.isComplete()) {
        logger.trace("Successfully authenticated to server using {}", context.saslClient.getMechanismName());
        // setup security layers here..
        return null;
      } else {

        // server completed before client; so try once, fail otherwise
        evaluateChallenge(context.ugi, context.saslClient,
            context.challenge.getData().toByteArray()); // discard response

        if (context.saslClient.isComplete()) {
          logger.trace("Successfully authenticated to server using {}", context.saslClient.getMechanismName());
          // setup security layers here..
          return null;
        } else {
          throw new SaslException("Server allegedly succeeded authentication, but client did not. Suspicious?");
        }
      }
    }
  }

  private static class SaslFailedProcessor implements SaslChallengeProcessor {

    @Override
    public SaslMessage process(SaslChallengeContext context) throws Exception {
      throw new SaslException("Authentication failed. Incorrect credentials?");
    }
  }

  private static byte[] evaluateChallenge(final UserGroupInformation ugi, final SaslClient saslClient,
                                          final byte[] challengeBytes) throws SaslException {
    try {
      return ugi.doAs(new PrivilegedExceptionAction<byte[]>() {
        @Override
        public byte[] run() throws Exception {
          return saslClient.evaluateChallenge(challengeBytes);
        }
      });
    } catch (final UndeclaredThrowableException e) {
      throw new SaslException(
          String.format("Unexpected failure (%s)", saslClient.getMechanismName()), e.getCause());
    } catch (final IOException | InterruptedException e) {
      if (e instanceof SaslException) {
        throw (SaslException) e;
      } else {
        throw new SaslException(
            String.format("Unexpected failure (%s)", saslClient.getMechanismName()), e);
      }
    }
  }
}
