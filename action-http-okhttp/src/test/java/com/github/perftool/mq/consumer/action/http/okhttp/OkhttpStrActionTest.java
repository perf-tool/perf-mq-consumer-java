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

package com.github.perftool.mq.consumer.action.http.okhttp;

import com.github.perftool.mq.consumer.action.http.common.ActionHttpConfig;
import com.github.perftool.mq.consumer.action.module.ActionMsg;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.util.Optional;

class OkhttpStrActionTest {
    private static MockWebServer server;

    private OkhttpStrAction action;

    @BeforeEach
    public void setUp() {
        ActionHttpConfig config = new ActionHttpConfig();
        config.host = "localhost";
        config.port = 30000;
        config.uri = "receive";
        config.maxIdleConnections = 10;
        config.connectionKeepaliveMinutes = 10;
        config.connectionTimeoutSeconds = 30;
        config.requestTimeoutSeconds = 30;
        action = new OkhttpStrAction(config);
        action.init();
    }

    @AfterAll
    static void shutdown() throws Exception {
        server.shutdown();
    }

    @BeforeAll
    public static void startMockHttpServer() throws Exception {
        server = new MockWebServer();
        server.enqueue(new MockResponse().setResponseCode(200));
        server.start(InetAddress.getByName("localhost"), 30000);
    }

    @Test
    void handleMsgTest() throws InterruptedException {
        ActionMsg<String> msg1 = new ActionMsg<>();
        msg1.setContent("{\"k1\":\"v1\"}");
        msg1.setMessageId("0");
        action.handleMsg(msg1, Optional.empty());
        RecordedRequest request1 = server.takeRequest();
        Assertions.assertEquals("/receive", request1.getPath());
        Assertions.assertEquals("[text={\"k1\":\"v1\"}]", request1.getBody().toString());

        ActionMsg<String> msg2 = new ActionMsg<>();
        msg2.setContent("{\"k2\":\"v2\"}");
        msg2.setMessageId("1");
        action.handleMsg(msg2, Optional.empty());
        RecordedRequest request2 = server.takeRequest();
        Assertions.assertEquals("/receive", request2.getPath());
        Assertions.assertEquals("[text={\"k2\":\"v2\"}]", request2.getBody().toString());
    }
}
