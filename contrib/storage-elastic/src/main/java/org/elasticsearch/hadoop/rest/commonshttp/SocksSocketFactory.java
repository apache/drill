/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.hadoop.rest.commonshttp;

import java.io.IOException;
import java.net.Authenticator;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.PasswordAuthentication;
import java.net.Proxy;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.UnknownHostException;

import org.apache.commons.httpclient.ConnectTimeoutException;
import org.apache.commons.httpclient.params.HttpConnectionParams;
import org.apache.commons.httpclient.protocol.DefaultProtocolSocketFactory;
import org.elasticsearch.hadoop.util.StringUtils;

class SocksSocketFactory extends DefaultProtocolSocketFactory {

    private final String socksHost;
    private final int socksPort;

    SocksSocketFactory(String socksHost, int socksPort) {
        this(socksHost, socksPort, null, null);
    }

    SocksSocketFactory(String socksHost, int socksPort, final String user, final String pass) {
        this.socksHost = socksHost;
        this.socksPort = socksPort;

        if (StringUtils.hasText(user)) {
            final PasswordAuthentication auth = new PasswordAuthentication(user, pass.toCharArray());

            Authenticator.setDefault(new Authenticator() {
                @Override
                protected PasswordAuthentication getPasswordAuthentication() {
                    return auth;
                }
            });
        }
    }

    public Socket createSocket(final String host, final int port, final InetAddress localAddress, final int localPort, final HttpConnectionParams params)
            throws IOException, UnknownHostException, ConnectTimeoutException {

        InetSocketAddress socksAddr = new InetSocketAddress(socksHost, socksPort);
        Proxy proxy = new Proxy(Proxy.Type.SOCKS, socksAddr);
        int timeout = params.getConnectionTimeout();

        Socket socket = new Socket(proxy);
        socket.setSoTimeout(timeout);

        SocketAddress localaddr = new InetSocketAddress(localAddress, localPort);
        SocketAddress remoteaddr = new InetSocketAddress(host, port);
        socket.bind(localaddr);
        socket.connect(remoteaddr, timeout);
        return socket;
    }

    public int hashCode() {
        return SocksSocketFactory.class.hashCode();
    }
}