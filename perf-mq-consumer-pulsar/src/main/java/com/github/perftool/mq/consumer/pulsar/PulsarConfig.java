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

package com.github.perftool.mq.consumer.pulsar;

import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Configuration
@Service
public class PulsarConfig {

    @Value("${PULSAR_HOST:localhost}")
    public String host;

    @Value("${PULSAR_PORT:8080}")
    public int port;

    @Value("${PULSAR_IO_THREADS:4}")
    public int ioThreads;

    @Value("${PULSAR_TENANT:public}")
    public String tenant;

    @Value("${PULSAR_TENANT_PREFIX:}")
    public String tenantPrefix;

    @Value("${PULSAR_TENANT_SUFFIX_NUM:0}")
    public int tenantSuffixNum;

    @Value("${PULSAR_TENANT_SUFFIX_NUM_OF_DIGITS:0}")
    public int tenantSuffixNumOfDigits;

    @Value("${PULSAR_NAMESPACE:default}")
    public String namespace;

    @Value("${PULSAR_NAMESPACE_PREFIX:}")
    public String namespacePrefix;

    @Value("${PULSAR_NAMESPACE_SUFFIX_NUM:0}")
    public int namespaceSuffixNum;

    @Value("${PULSAR_NAMESPACE_SUFFIX_NUM_OF_DIGITS:0}")
    public int namespaceSuffixNumOfDigits;

    @Value("${PULSAR_TOPIC:topic}")
    public String topic;

    @Value("${PULSAR_TOPIC_SUFFIX_NUM:0}")
    public int topicSuffixNum;

    @Value("${PULSAR_TOPIC_SUFFIX_NUM_OF_DIGITS:0}")
    public int topicSuffixNumOfDigits;

    @Value("${PULSAR_AUTO_UPDATE_PARTITION:false}")
    public boolean autoUpdatePartition;

    @Value("${PULSAR_AUTO_UPDATE_PARTITION_SECONDS:60}")
    public int autoUpdatePartitionSeconds;

    @Value("${PULSAR_OPERATION_TIMEOUT_SECONDS:15}")
    public int operationTimeoutSeconds;

    @Value("${PULSAR_SUBSCRIPTION_TYPE:Failover}")
    public SubscriptionType subscriptionType;

    @Value("${PULSAR_SUBSCRIPTION_NAME:}")
    public String subscriptionName;

    @Value("${PULSAR_SUBSCRIPTION_INITIAL_POSITION:Latest}")
    public SubscriptionInitialPosition subscriptionInitialPosition;

    @Value("${PULSAR_SUBSCRIPTION_SEEK_TIMESTAMP:0}")
    public long subscriptionSeekTimestamp;

    @Value("${PULSAR_RECEIVE_QUEUE_SIZE:1000}")
    public int receiveQueueSize;

    @Value("${PULSAR_SYNC_RECEIVE_TIMEOUT_MS:0}")
    public int syncReceiveTimeoutMs;

    @Value("${PULSAR_CONSUME_BATCH:false}")
    public boolean consumeBatch;

    @Value("${PULSAR_CONSUME_BATCH_TIMEOUT_MS:50}")
    public int consumeBatchTimeoutMs;

    @Value("${PULSAR_CONSUME_BATCH_MAX_MESSAGES:500}")
    public int consumeBatchMaxMessages;

    @Value("${PULSAR_CONSUME_ASYNC:false}")
    public boolean consumeAsync;

    @Value("${PULSAR_CONSUME_RATE_LIMITER:-1}")
    public int rateLimiter;

    @Value("${PULSAR_CONSUME_RECEIVE_LIMITER:-1}")
    public int receiveLimiter;

    @Value("${PULSAR_ENABLE_ACK_TIMEOUT:false}")
    public boolean enableAckTimeout;

    @Value("${PULSAR_ACK_TIMEOUT_MILLISECONDS:30000}")
    public int ackTimeoutMilliseconds;

    @Value("${PULSAR_ACK_TIMEOUT_TICK_TIME_MILLISECONDS:100}")
    public int ackTimeoutTickTimeMilliseconds;

    @Value("${PULSAR_TLS_ENABLE:false}")
    public boolean tlsEnable;

    @Value("${PULSAR_KEYSTORE_PATH:}")
    public String keyStorePath;

    @Value("${PULSAR_KEYSTORE_PASSWORD:}")
    public String keyStorePassword;

    @Value("${PULSAR_TLS_TRUSTSTORE_PATH:}")
    public String tlsTrustStorePath;

    @Value("${PULSAR_TLS_TRUSTSTORE_PASSWORD:}")
    public String tlsTrustStorePassword;

    @Value("${PULSAR_PRINT_LOG_MSG_DELAY_MS:1000}")
    public int printLogMsgDelayMs;

    public String getSubscriptionName() {
        if (StringUtils.isEmpty(subscriptionName)) {
            return UUID.randomUUID().toString();
        }
        return subscriptionName;
    }
}
