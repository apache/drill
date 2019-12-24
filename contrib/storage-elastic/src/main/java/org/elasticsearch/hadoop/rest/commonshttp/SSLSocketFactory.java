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
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

import javax.net.SocketFactory;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

import org.apache.commons.httpclient.ConnectTimeoutException;
import org.apache.commons.httpclient.params.HttpConnectionParams;
import org.apache.commons.httpclient.protocol.SecureProtocolSocketFactory;
import org.elasticsearch.hadoop.EsHadoopIllegalStateException;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.util.IOUtils;
import org.elasticsearch.hadoop.util.StringUtils;

class SSLSocketFactory implements SecureProtocolSocketFactory {

    private static class TrustManagerDelegate implements X509TrustManager {

        private final X509TrustManager trustManager;
        private final TrustStrategy trustStrategy;

        TrustManagerDelegate(final X509TrustManager trustManager, final TrustStrategy trustStrategy) {
            super();
            this.trustManager = trustManager;
            this.trustStrategy = trustStrategy;
        }

        public void checkClientTrusted(final X509Certificate[] chain, final String authType)
                throws CertificateException {
            this.trustManager.checkClientTrusted(chain, authType);
        }

        public void checkServerTrusted(final X509Certificate[] chain, final String authType)
                throws CertificateException {
            if (!this.trustStrategy.isTrusted(chain, authType)) {
                this.trustManager.checkServerTrusted(chain, authType);
            }
        }

        public X509Certificate[] getAcceptedIssuers() {
            return this.trustManager.getAcceptedIssuers();
        }

    }

    private static interface TrustStrategy {
        boolean isTrusted(X509Certificate[] chain, String authType) throws CertificateException;
    }

    private static class SelfSignedStrategy implements TrustStrategy {
        public boolean isTrusted(X509Certificate[] chain, String authType) throws CertificateException {
            return chain.length == 1;
        }
    }

    private SSLContext sslContext = null;

    private final String sslProtocol;

    private final String keyStoreLocation;
    private final String keyStorePass;
    private final String keyStoreType;

    private final String trustStoreLocation;
    private final String trustStorePass;
    private final TrustStrategy trust;

    SSLSocketFactory(Settings settings) {
        sslProtocol = settings.getNetworkSSLProtocol();

        keyStoreLocation = settings.getNetworkSSLKeyStoreLocation();
        keyStorePass = settings.getNetworkSSLKeyStorePass();
        keyStoreType = settings.getNetworkSSLKeyStoreType();

        trustStoreLocation = settings.getNetworkSSLTrustStoreLocation();
        trustStorePass = settings.getNetworkSSLTrustStorePass();

        trust = (settings.getNetworkSSLAcceptSelfSignedCert() ? new SelfSignedStrategy() : null);
    }

    @Override
    public Socket createSocket(String host, int port, InetAddress localAddress, int localPort, HttpConnectionParams params) throws IOException, UnknownHostException, ConnectTimeoutException {
        if (params == null) {
            throw new IllegalArgumentException("Parameters may not be null");
        }
        int timeout = params.getConnectionTimeout();
        SocketFactory socketfactory = getSSLContext().getSocketFactory();
        if (timeout == 0) {
            return socketfactory.createSocket(host, port, localAddress, localPort);
        }
        else {
            Socket socket = socketfactory.createSocket();
            SocketAddress localaddr = new InetSocketAddress(localAddress, localPort);
            SocketAddress remoteaddr = new InetSocketAddress(host, port);
            socket.bind(localaddr);
            socket.connect(remoteaddr, timeout);
            return socket;
        }
    }

    @Override
    public Socket createSocket(String host, int port) throws IOException, UnknownHostException {
        return getSSLContext().getSocketFactory().createSocket(host, port);
    }

    @Override
    public Socket createSocket(Socket socket, String host, int port, boolean autoClose) throws IOException, UnknownHostException {
        return getSSLContext().getSocketFactory().createSocket(socket, host, port, autoClose);
    }

    @Override
    public Socket createSocket(String host, int port, InetAddress localAddress, int localPort) throws IOException, UnknownHostException {
        return getSSLContext().getSocketFactory().createSocket(host, port, localAddress, localPort);
    }

    private SSLContext getSSLContext() {
        if (sslContext == null) {
            sslContext = createSSLContext();
        }
        return sslContext;
    }

    private SSLContext createSSLContext() {
        SSLContext ctx;
        try {
            ctx = SSLContext.getInstance(sslProtocol);
        } catch (NoSuchAlgorithmException ex) {
            throw new EsHadoopIllegalStateException("Cannot instantiate SSL - " + ex.getMessage(), ex);
        }
        try {
            ctx.init(loadKeyManagers(), loadTrustManagers(), null);
        } catch (Exception ex) {
            throw new EsHadoopIllegalStateException("Cannot initialize SSL - " + ex.getMessage(), ex);
        }

        return ctx;
    }

    private KeyStore loadKeyStore(String location, char[] pass) throws GeneralSecurityException, IOException {
        KeyStore keyStore = KeyStore.getInstance(keyStoreType);
        InputStream in = null;
        try {
            in = IOUtils.open(location);
            keyStore.load(in, pass);
        }
        finally {
            IOUtils.close(in);
        }
        return keyStore;
    }

    private KeyManager[] loadKeyManagers() throws GeneralSecurityException, IOException {
        if (!StringUtils.hasText(keyStoreLocation)) {
            return null;
        }

        char[] pass = (StringUtils.hasText(keyStorePass) ? keyStorePass.trim().toCharArray() : null);
        KeyStore keyStore = loadKeyStore(keyStoreLocation, pass);
        KeyManagerFactory kmFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmFactory.init(keyStore, pass);
        return kmFactory.getKeyManagers();
    }

    private TrustManager[] loadTrustManagers() throws GeneralSecurityException, IOException {
        KeyStore keyStore = null;

        if (StringUtils.hasText(trustStoreLocation)) {
            char[] pass = (StringUtils.hasText(trustStorePass) ? trustStorePass.trim().toCharArray() : null);
            keyStore = loadKeyStore(trustStoreLocation, pass);
        }

        TrustManagerFactory tmFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmFactory.init(keyStore);
        TrustManager[] tms = tmFactory.getTrustManagers();

        if (tms != null && trust != null) {
            // be defensive since the underlying impl might not give us a copy
            TrustManager[] clone = new TrustManager[tms.length];

            for (int i = 0; i < tms.length; i++) {
                TrustManager tm = tms[i];
                if (tm instanceof X509TrustManager) {
                    tm = new TrustManagerDelegate((X509TrustManager) tm, trust);
                }
                clone[i] = tm;
            }
            tms = clone;
        }

        return tms;
    }

    public boolean equals(Object obj) {
        return ((obj != null) && obj.getClass().equals(SSLSocketFactory.class));
    }

    public int hashCode() {
        return getClass().hashCode();
    }
}