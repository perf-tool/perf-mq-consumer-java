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

import com.github.perftool.mq.consumer.common.AbstractPullThread;
import com.github.perftool.mq.consumer.common.service.ActionService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

@Slf4j
public abstract class AbstractKafkaPullThread<T> extends AbstractPullThread {
    private final List<String> topics;

    private final KafkaConfig kafkaConfig;

    private KafkaConsumer<T, T> consumer;

    private long lastPollTime;

    private final HashMap<Integer, Long> partitionOffsetMap;

    private KafkaConsumer<T, T> buildConsumer(List<String> topics, KafkaConfig kafkaConfig) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfig.addr);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, kafkaConfig.groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, kafkaConfig.autoOffsetResetConfig);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, kafkaConfig.maxPollRecords);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, kafkaConfig.fetchMinBytes);
        props.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, kafkaConfig.fetchMaxKb * 1024);
        props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, kafkaConfig.partitionFetchMaxKb * 1024);
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, kafkaConfig.maxFetchWaitMs);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, getKeyDeserializerName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, getValueDeserializerName());
        if (kafkaConfig.saslEnable) {
            if (kafkaConfig.saslSslEnable) {
                props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, kafkaConfig.saslSslTrustStoreLocation);
                props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, kafkaConfig.saslSslTrustStorePassword);
                props.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "");
                props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_SSL.name);
            } else {
                props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.name);
            }
            props.put(SaslConfigs.SASL_MECHANISM, kafkaConfig.saslMechanism);
            String saslJaasConfig = String.format(
                    "org.apache.kafka.common.security.plain.PlainLoginModule required %n"
                            + "username=\"%s\" %npassword=\"%s\";",
                    kafkaConfig.saslUsername, kafkaConfig.saslPassword);
            props.put(SaslConfigs.SASL_JAAS_CONFIG, saslJaasConfig);
        }
        KafkaConsumer<T, T> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(topics);
        return consumer;
    }

    public AbstractKafkaPullThread(int i, ActionService actionService, List<String> topics, KafkaConfig kafkaConfig) {
        super(i, actionService);
        this.topics = topics;
        this.kafkaConfig = kafkaConfig;
        partitionOffsetMap = new HashMap<>();
        this.consumer = buildConsumer(topics, kafkaConfig);
    }

    protected abstract String getKeyDeserializerName();

    protected abstract String getValueDeserializerName();

    @Override
    protected void pull() {
        try {
            ConsumerRecords<T, T> consumerRecords = consumer.poll(Duration.ofMillis(kafkaConfig.pollMs));
            if (System.currentTimeMillis() - lastPollTime > TimeUnit.SECONDS.toMillis(1)) {
                log.warn("topics {} the last poll message is greater than 1 second", topics);
            }
            lastPollTime = System.currentTimeMillis();
            for (ConsumerRecord<T, T> record : consumerRecords) {
                Long lastOffset = partitionOffsetMap.getOrDefault(record.partition(), 0L);
                if (record.offset() - lastOffset > 1) {
                    log.warn("partition {} offset jump, from {} to {} diff {}",
                            record.partition(), lastOffset, record.offset(), record.offset() - lastOffset);
                }
                partitionOffsetMap.put(record.partition(), record.offset());
                log.debug("receive a record, offset is [{}]", record.offset());
                this.handle(record);
            }
        } catch (KafkaException e) {
            if (e.getMessage().contains("Record is corrupt")) {
                this.consumer = buildConsumer(this.topics, this.kafkaConfig);
            }
        }
    }

    protected abstract void handle(ConsumerRecord<T, T> record);

}
