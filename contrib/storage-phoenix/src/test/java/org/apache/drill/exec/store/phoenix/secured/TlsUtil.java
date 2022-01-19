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
package org.apache.drill.exec.store.phoenix.secured;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.ssl.FileBasedKeyStoresFactory;
import org.apache.hadoop.security.ssl.SSLFactory;
import org.bouncycastle.x509.X509V1CertificateGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.x500.X500Principal;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.math.BigInteger;
import java.net.URL;
import java.security.GeneralSecurityException;
import java.security.InvalidKeyException;
import java.security.Key;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.SignatureException;
import java.security.cert.Certificate;
import java.security.cert.CertificateEncodingException;
import java.security.cert.X509Certificate;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class TlsUtil {

  protected static final String KEYSTORE_PASSWORD = "avaticasecret";
  protected static final String TRUSTSTORE_PASSWORD = "avaticasecret";

  protected static final String TARGET_DIR_NAME = System.getProperty("target.dir", "target");
  protected static final File TARGET_DIR = new File(System.getProperty("user.dir"), TARGET_DIR_NAME);
  protected static final File KEYSTORE = new File(TARGET_DIR, "avatica-test-ks.jks");
  protected static final File TRUSTSTORE = new File(TARGET_DIR, "avatica-test-ts.jks");

  private static final Logger LOG = LoggerFactory.getLogger(TlsUtil.class);

  public static File getTrustStoreFile() {
      return TRUSTSTORE;
  }

  public static File getKeyStoreFile() {
      return KEYSTORE;
  }

  public static String getTrustStorePassword() {
      return TRUSTSTORE_PASSWORD;
  }

  public static String getKeyStorePassword() {
      return KEYSTORE_PASSWORD;
  }

  static {
    try {
      setupTls();
    } catch (Exception e) {
      LOG.error("could not set upt TLS for HTTPS tests", e);
      throw new RuntimeException(e);
    }
  }

  /**
   * Performs setup of SSL configuration in preparation for testing an SSLFactory. This includes
   * keys, certs, keystores, truststores.
   */
  public static void setupTls() throws Exception {
    try {
      KEYSTORE.delete();
    } catch (Exception e) {
      // may not exist
    }

    try {
      TRUSTSTORE.delete();
    } catch (Exception e) {
      // may not exist
    }

    KeyPair sKP = generateKeyPair("RSA");
    X509Certificate sCert =
      generateCertificate("CN=localhost, O=server", sKP, 30, "SHA1withRSA");
    createKeyStore(KEYSTORE.getCanonicalPath(), KEYSTORE_PASSWORD, "server", sKP.getPrivate(), sCert);

    Map<String, X509Certificate> certs = new HashMap<String, X509Certificate>();
    certs.put("server", sCert);

    createTrustStore(TRUSTSTORE.getCanonicalPath(), TRUSTSTORE_PASSWORD, certs);
  }

  /**
   * Create a self-signed X.509 Certificate.
   *
   * @param dn the X.509 Distinguished Name, eg "CN=Test, L=London, C=GB"
   * @param pair the KeyPair
   * @param days how many days from now the Certificate is valid for
   * @param algorithm the signing algorithm, eg "SHA1withRSA"
   * @return the self-signed certificate
   */
  private static X509Certificate generateCertificate(String dn, KeyPair pair, int days, String algorithm)
      throws CertificateEncodingException, InvalidKeyException, IllegalStateException, NoSuchAlgorithmException,
      SignatureException {
    Date from = new Date();
    Date to = new Date(from.getTime() + days * 86400000L);
    BigInteger sn = new BigInteger(64, new SecureRandom());
    KeyPair keyPair = pair;
    X509V1CertificateGenerator certGen = new X509V1CertificateGenerator();
    X500Principal dnName = new X500Principal(dn);

    certGen.setSerialNumber(sn);
    certGen.setIssuerDN(dnName);
    certGen.setNotBefore(from);
    certGen.setNotAfter(to);
    certGen.setSubjectDN(dnName);
    certGen.setPublicKey(keyPair.getPublic());
    certGen.setSignatureAlgorithm(algorithm);
    X509Certificate cert = certGen.generate(pair.getPrivate());
    return cert;
  }

  public static String getClasspathDir(Class<?> klass) throws Exception {
    String file = klass.getName();
    file = file.replace('.', '/') + ".class";
    URL url = Thread.currentThread().getContextClassLoader().getResource(file);
    String baseDir = url.toURI().getPath();
    baseDir = baseDir.substring(0, baseDir.length() - file.length() - 1);
    return baseDir;
  }

  /**
   * Performs complete setup of SSL configuration in preparation for testing an
   * SSLFactory.  This includes keys, certs, keystores, truststores, the server
   * SSL configuration file, the client SSL configuration file, and the master
   * configuration file read by the SSLFactory.
   *
   * @param keystoresDir String directory to save keystores
   * @param sslConfDir String directory to save SSL configuration files
   * @param conf Configuration master configuration to be used by an SSLFactory,
   *   which will be mutated by this method
   * @param useClientCert boolean true to make the client present a cert in the
   *   SSL handshake
   */
  public static void setupSSLConfig(String keystoresDir, String sslConfDir, Configuration conf, boolean useClientCert)
      throws Exception {
    String clientKS = keystoresDir + "/clientKS.jks";
    String clientPassword = "clientP";
    String serverKS = keystoresDir + "/serverKS.jks";
    String serverPassword = "serverP";
    String trustKS = keystoresDir + "/trustKS.jks";
    String trustPassword = "trustP";

    File sslClientConfFile = new File(sslConfDir + "/ssl-client.xml");
    File sslServerConfFile = new File(sslConfDir + "/ssl-server.xml");

    Map<String, X509Certificate> certs = new HashMap<>();

    if (useClientCert) {
      KeyPair cKP = generateKeyPair("RSA");
      X509Certificate cCert =
              generateCertificate("CN=localhost, O=client", cKP, 30, "SHA1withRSA");
      createKeyStore(clientKS, clientPassword, "client", cKP.getPrivate(), cCert);
      certs.put("client", cCert);
    }

    KeyPair sKP = generateKeyPair("RSA");
    X509Certificate sCert = generateCertificate("CN=localhost, O=server", sKP, 30, "SHA1withRSA");
    createKeyStore(serverKS, serverPassword, "server", sKP.getPrivate(), sCert);
    certs.put("server", sCert);

    createTrustStore(trustKS, trustPassword, certs);

    Configuration clientSSLConf = createClientSSLConfig(clientKS, clientPassword, clientPassword, trustKS);
    Configuration serverSSLConf = createServerSSLConfig(serverKS, serverPassword, serverPassword, trustKS);

    saveConfig(sslClientConfFile, clientSSLConf);
    saveConfig(sslServerConfFile, serverSSLConf);

    conf.set(SSLFactory.SSL_HOSTNAME_VERIFIER_KEY, "ALLOW_ALL");
    conf.set(SSLFactory.SSL_CLIENT_CONF_KEY, sslClientConfFile.getName());
    conf.set(SSLFactory.SSL_SERVER_CONF_KEY, sslServerConfFile.getName());
    conf.setBoolean(SSLFactory.SSL_REQUIRE_CLIENT_CERT_KEY, useClientCert);
  }

  /**
   * Creates SSL configuration for a client.
   *
   * @param clientKS String client keystore file
   * @param password String store password, or null to avoid setting store
   *   password
   * @param keyPassword String key password, or null to avoid setting key
   *   password
   * @param trustKS String truststore file
   * @return Configuration for client SSL
   */
  private static Configuration createClientSSLConfig(String clientKS, String password, String keyPassword,
                                                     String trustKS) {
    Configuration clientSSLConf = createSSLConfig(SSLFactory.Mode.CLIENT, clientKS, password, keyPassword, trustKS);
    return clientSSLConf;
  }

  /**
   * Creates SSL configuration for a server.
   *
   * @param serverKS String server keystore file
   * @param password String store password, or null to avoid setting store
   *   password
   * @param keyPassword String key password, or null to avoid setting key
   *   password
   * @param trustKS String truststore file
   * @return Configuration for server SSL
   */
  private static Configuration createServerSSLConfig(String serverKS, String password, String keyPassword,
                                                     String trustKS) throws IOException {
      Configuration serverSSLConf = createSSLConfig(SSLFactory.Mode.SERVER, serverKS, password, keyPassword, trustKS);
      return serverSSLConf;
  }

  /**
   * Creates SSL configuration.
   *
   * @param mode SSLFactory.Mode mode to configure
   * @param keystore String keystore file
   * @param password String store password, or null to avoid setting store
   *   password
   * @param keyPassword String key password, or null to avoid setting key
   *   password
   * @param trustKS String truststore file
   * @return Configuration for SSL
   */
  private static Configuration createSSLConfig(SSLFactory.Mode mode, String keystore, String password,
                                               String keyPassword, String trustKS) {
    String trustPassword = "trustP";

    Configuration sslConf = new Configuration(false);
    if (keystore != null) {
      sslConf.set(FileBasedKeyStoresFactory.resolvePropertyName(mode,
        FileBasedKeyStoresFactory.SSL_KEYSTORE_LOCATION_TPL_KEY), keystore);
    }
    if (password != null) {
      sslConf.set(FileBasedKeyStoresFactory.resolvePropertyName(mode,
        FileBasedKeyStoresFactory.SSL_KEYSTORE_PASSWORD_TPL_KEY), password);
    }
    if (keyPassword != null) {
      sslConf.set(FileBasedKeyStoresFactory.resolvePropertyName(mode,
        FileBasedKeyStoresFactory.SSL_KEYSTORE_KEYPASSWORD_TPL_KEY), keyPassword);
    }
    if (trustKS != null) {
      sslConf.set(FileBasedKeyStoresFactory.resolvePropertyName(mode,
        FileBasedKeyStoresFactory.SSL_TRUSTSTORE_LOCATION_TPL_KEY), trustKS);
    }
    if (trustPassword != null) {
      sslConf.set(FileBasedKeyStoresFactory.resolvePropertyName(mode,
        FileBasedKeyStoresFactory.SSL_TRUSTSTORE_PASSWORD_TPL_KEY), trustPassword);
    }
    sslConf.set(FileBasedKeyStoresFactory.resolvePropertyName(mode,
      FileBasedKeyStoresFactory.SSL_TRUSTSTORE_RELOAD_INTERVAL_TPL_KEY), "1000");

    return sslConf;
  }

  /**
   * Saves configuration to a file.
   *
   * @param file File to save
   * @param conf Configuration contents to write to file
   * @throws IOException if there is an I/O error saving the file
   */
  private static void saveConfig(File file, Configuration conf) throws IOException {
    Writer writer = new FileWriter(file);
    try {
      conf.writeXml(writer);
    } finally {
      writer.close();
    }
  }

  private static KeyPair generateKeyPair(String algorithm) throws NoSuchAlgorithmException {
    KeyPairGenerator keyGen = KeyPairGenerator.getInstance(algorithm);
    keyGen.initialize(1024);
    return keyGen.genKeyPair();
  }

  private static KeyStore createEmptyKeyStore() throws GeneralSecurityException, IOException {
    KeyStore ks = KeyStore.getInstance("JKS");
    ks.load(null, null); // initialize
    return ks;
  }

  private static void saveKeyStore(KeyStore ks, String filename, String password)
      throws GeneralSecurityException, IOException {
    FileOutputStream out = new FileOutputStream(filename);
    try {
      ks.store(out, password.toCharArray());
    } finally {
      out.close();
    }
  }

  private static void createKeyStore(String filename, String password, String alias, Key privateKey, Certificate cert)
      throws GeneralSecurityException, IOException {
    KeyStore ks = createEmptyKeyStore();
    ks.setKeyEntry(alias, privateKey, password.toCharArray(), new Certificate[] { cert });
    saveKeyStore(ks, filename, password);
  }

  private static <T extends Certificate> void createTrustStore(String filename, String password, Map<String, T> certs)
      throws GeneralSecurityException, IOException {
    KeyStore ks = createEmptyKeyStore();
    for (Map.Entry<String, T> cert : certs.entrySet()) {
      ks.setCertificateEntry(cert.getKey(), cert.getValue());
    }
    saveKeyStore(ks, filename, password);
  }
}
