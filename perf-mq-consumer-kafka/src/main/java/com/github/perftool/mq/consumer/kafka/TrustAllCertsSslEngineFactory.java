/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.github.perftool.mq.consumer.kafka;

import org.apache.kafka.common.security.auth.SslEngineFactory;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;

public class TrustAllCertsSslEngineFactory implements SslEngineFactory {
    private static final BiFunction<String, Integer, SSLEngine> TRUST_ALL_SSL_ENGINE = (peerHost, peerPort) -> {
        try {
            TrustManager[] trustAllCerts = new TrustManager[]{new X509TrustManager() {
                @Override
                public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {

                }

                @Override
                public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {

                }

                @Override
                public X509Certificate[] getAcceptedIssuers() {
                    return null;
                }
            }};
            SSLContext sc = SSLContext.getInstance("SSL");
            sc.init(null, trustAllCerts, null);
            SSLEngine sslEngine = sc.createSSLEngine(peerHost, peerPort);
            sslEngine.setUseClientMode(true);
            return sslEngine;
        } catch (NoSuchAlgorithmException | KeyManagementException e) {
            throw new RuntimeException(e);
        }
    };

    @Override
    public SSLEngine createClientSslEngine(String peerHost, int peerPort, String endpointIdentification) {
        return TRUST_ALL_SSL_ENGINE.apply(peerHost, peerPort);
    }

    @Override
    public SSLEngine createServerSslEngine(String peerHost, int peerPort) {
        return TRUST_ALL_SSL_ENGINE.apply(peerHost, peerPort);
    }

    @Override
    public boolean shouldBeRebuilt(Map<String, Object> map) {
        return false;
    }

    @Override
    public Set<String> reconfigurableConfigs() {
        return null;
    }

    @Override
    public KeyStore keystore() {
        return null;
    }

    @Override
    public KeyStore truststore() {
        return null;
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
