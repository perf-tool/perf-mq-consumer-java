/**
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

package com.github.perftool.mq.consumer.action.pulsar;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Service;

@Configuration
@Service
public class ActionPulsarConfig {
    @Value("${ACTION_PULSAR_ADDR:}")
    public String addr;

    @Value("${ACTION_PULSAR_TOPIC:}")
    public String topic;

    @Value("${PULSAR_IO_THREADS:4}")
    public int pulsarIoThreads;

    @Value("${PULSAR_MAX_CONCURRENT_LOOKUP_REQUESTS:5000}")
    public int pulsarMaxConcurrentLookupRequests;

    @Value("${PULSAR_MEMORY_LIMIT_MB:50}")
    public int memoryLimitMb;

    @Value("${PULSAR_ENABLE_BATCHING:true}")
    public boolean enableBatching;

    @Value("${PULSAR_BATCHING_MAX_BYTES:131072}")
    public int batchingMaxBytes;

    @Value("${PULSAR_BATCHING_MAX_MESSAGES:1000}")
    public int batchingMaxMessages;

    @Value("${PULSAR_BATCHING_MAX_PUBLISH_DELAY_MS:1}")
    public long batchingMaxPublishDelay;
}
