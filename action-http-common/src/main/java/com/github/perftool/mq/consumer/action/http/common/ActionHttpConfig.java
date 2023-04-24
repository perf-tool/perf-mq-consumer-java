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

package com.github.perftool.mq.consumer.action.http.common;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Service;

@Configuration
@Service
public class ActionHttpConfig {

    @Value("${ACTION_HTTP_HOST:localhost}")
    public String host;

    @Value("${ACTION_HTTP_PORT:80}")
    public int port;

    @Value("${ACTION_HTTP_URI:receive}")
    public String uri;

    @Value("${ACTION_HTTP_REQUEST_TIMEOUT_SECONDS:30}")
    public int requestTimeoutSeconds;

    @Value("${ACTION_HTTP_CONNECTION_TIMEOUT_SECONDS:30}")
    public int connectionTimeoutSeconds;

    @Value("${ACTION_HTTP_MAX_IDLE_CONNECTIONS:10}")
    public int maxIdleConnections;

    @Value("${ACTION_HTTP_CONNECTION_KEEPALIVE_MINUTES:10}")
    public int connectionKeepaliveMinutes;

    @Value("${ACTION_HTTP_ASYNC:false}")
    public boolean async;

}
