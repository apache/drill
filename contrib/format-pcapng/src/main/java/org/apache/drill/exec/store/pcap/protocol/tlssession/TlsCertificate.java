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
package org.apache.drill.exec.store.pcap.protocol.tlssession;

import java.io.ByteArrayInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.interfaces.DSAKey;
import java.security.interfaces.ECKey;
import java.security.interfaces.EdECKey;
import java.security.interfaces.RSAKey;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HexFormat;
import java.util.List;

import org.apache.drill.exec.store.pcap.protocol.DecoderContext;

/** Fields of one X.509 certificate from a TLS Certificate message. Null fields could not be read. */
public class TlsCertificate {
  static final int MAX_NAMES = 64;

  String subject;
  String issuer;
  String serial;
  Instant notBefore;
  Instant notAfter;
  final List<String> subjectAltNames = new ArrayList<>();
  String signatureAlgorithm;
  String publicKeyAlgorithm;
  Integer publicKeyBits;
  String sha256;
  Boolean isSelfSigned;

  /** Parses a DER certificate. A certificate that cannot be parsed keeps only its fingerprint and is warned about. */
  static TlsCertificate parse(byte[] der, int index, DecoderContext context) {
    TlsCertificate c = new TlsCertificate();
    c.sha256 = HexFormat.of().formatHex(sha256(der));
    X509Certificate x;
    try {
      x = (X509Certificate) CertificateFactory.getInstance("X.509").generateCertificate(new ByteArrayInputStream(der));
    } catch (Exception e) {
      context.warn("certificate " + index + " could not be parsed: " + TlsParser.cap(String.valueOf(e.getMessage())));
      return c;
    }
    c.subject = TlsParser.cap(x.getSubjectX500Principal().getName());
    c.issuer = TlsParser.cap(x.getIssuerX500Principal().getName());
    String serial = x.getSerialNumber().toString(16);
    c.serial = serial.length() % 2 == 0 ? serial : "0" + serial;
    c.notBefore = x.getNotBefore().toInstant();
    c.notAfter = x.getNotAfter().toInstant();
    c.signatureAlgorithm = x.getSigAlgName();
    PublicKey key = x.getPublicKey();
    c.publicKeyAlgorithm = key.getAlgorithm();
    c.publicKeyBits = keyBits(key);
    try {
      Collection<List<?>> names = x.getSubjectAlternativeNames();
      if (names != null) {
        for (List<?> name : names) {
          Object type = name.get(0);
          // 2 = dNSName, 7 = iPAddress
          if ((Integer.valueOf(2).equals(type) || Integer.valueOf(7).equals(type)) && name.get(1) instanceof String) {
            if (c.subjectAltNames.size() == MAX_NAMES) {
              context.warn("certificate " + index + ": kept the first " + MAX_NAMES + " subject alternative names");
              break;
            }
            c.subjectAltNames.add(TlsParser.cap((String) name.get(1)));
          }
        }
      }
    } catch (Exception e) {
      context.warn("certificate " + index + ": unreadable subject alternative names");
    }
    c.isSelfSigned = isSelfSigned(x);
    return c;
  }

  private static boolean isSelfSigned(X509Certificate x) {
    if (!x.getSubjectX500Principal().equals(x.getIssuerX500Principal())) {
      return false;
    }
    try {
      x.verify(x.getPublicKey());
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  private static Integer keyBits(PublicKey key) {
    if (key instanceof RSAKey) {
      return ((RSAKey) key).getModulus().bitLength();
    }
    if (key instanceof ECKey) {
      return ((ECKey) key).getParams().getOrder().bitLength();
    }
    if (key instanceof DSAKey && ((DSAKey) key).getParams() != null) {
      return ((DSAKey) key).getParams().getP().bitLength();
    }
    if (key instanceof EdECKey) {
      return ((EdECKey) key).getParams().getName().contains("448") ? 448 : 255;
    }
    return null;
  }

  private static byte[] sha256(byte[] data) {
    try {
      return MessageDigest.getInstance("SHA-256").digest(data);
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    }
  }
}
