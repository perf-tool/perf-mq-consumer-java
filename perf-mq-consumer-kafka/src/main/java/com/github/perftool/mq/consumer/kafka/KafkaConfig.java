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

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Service;

@Configuration
@Service
public class KafkaConfig {

    @Value("${KAFKA_ADDR:localhost:9092}")
    public String addr;

    @Value("${KAFKA_TOPIC:}")
    public String topic;

    @Value("${KAFKA_TOPIC_SUFFIX_NUM:0}")
    public int topicSuffixNum;

    @Value("${KAFKA_GROUP_ID:}")
    public String groupId;

    @Value("${KAFKA_AUTO_OFFSET_RESET_CONFIG:latest}")
    public String autoOffsetResetConfig;

    @Value("${KAFKA_MAX_POLL_RECORDS:500}")
    public int maxPollRecords;

    @Value("${KAFKA_POLL_MS:500}")
    public int pollMs;

    @Value("${KAFKA_MAX_FETCH_WAIT_MS:500}")
    public int maxFetchWaitMs;

    @Value("${KAFKA_FETCH_MIN_BYTES:1}")
    public int fetchMinBytes;

    @Value("${KAFKA_FETCH_MAX_KB:1024}")
    public int fetchMaxKb;

    @Value("${KAFKA_PARTITION_FETCH_MAX_KB:1024}")
    public int partitionFetchMaxKb;

    @Value("${KAFKA_SASL_ENABLE:false}")
    public boolean saslEnable;

    @Value("${KAFKA_SASL_MECHANISM:PLAIN}")
    public String saslMechanism;

    @Value("${KAFKA_SASL_USERNAME:}")
    public String saslUsername;

    @Value("${KAFKA_SASL_PASSWORD:}")
    public String saslPassword;

    @Value("${KAFKA_SASL_SSL_ENABLE:false}")
    public boolean saslSslEnable;

    @Value("${KAFKA_SASL_SSL_TRUSTSTORE_LOCATION:}")
    public String saslSslTrustStoreLocation;

    @Value("${KAFKA_SASL_SSL_TRUSTSTORE_PASSWORD:}")
    public String saslSslTrustStorePassword;

    @Value("${KAFKA_SASL_SSL_IGNORE_CERT:false}")
    public boolean saslSslIgnoreCertFile;

}
