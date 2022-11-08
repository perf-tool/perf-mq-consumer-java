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

package com.github.perftool.mq.consumer.pulsar;

import com.github.perftool.mq.consumer.action.module.ActionMsg;
import com.github.perftool.mq.consumer.common.config.CommonConfig;
import com.github.perftool.mq.consumer.common.metrics.E2EMetricsBean;
import com.github.perftool.mq.consumer.common.module.ConsumeMode;
import com.github.perftool.mq.consumer.common.module.ExchangeType;
import com.github.perftool.mq.consumer.common.service.ActionService;
import com.github.perftool.mq.consumer.common.util.NameUtil;
import com.github.perftool.mq.consumer.common.util.ThreadPool;
import io.github.perftool.trace.report.ITraceReporter;
import io.github.perftool.trace.report.ReportUtil;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.BatchReceivePolicy;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.MessageListener;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
public class PulsarBootService {

    private PulsarClient pulsarClient;

    private final PulsarConfig pulsarConfig;

    private final CommonConfig commonConfig;

    private final ActionService actionService;

    private static final String AUTH_PLUGIN_CLASS_NAME = "org.apache.pulsar.client.impl.auth.AuthenticationKeyStoreTls";

    private final ExecutorService executor;

    private final E2EMetricsBean e2EMetricsBean;

    private final ITraceReporter traceReporter;

    public PulsarBootService(@Autowired PulsarConfig pulsarConfig, @Autowired CommonConfig commonConfig,
                             @Autowired ActionService actionService, @Autowired ThreadPool threadPool,
                             @Autowired MeterRegistry meterRegistry) {
        this.pulsarConfig = pulsarConfig;
        this.commonConfig = commonConfig;
        this.actionService = actionService;
        this.executor = threadPool.create("pf-pulsar-consumer");
        this.traceReporter = ReportUtil.getReporter();
        this.e2EMetricsBean = new E2EMetricsBean(meterRegistry, "pulsar", pulsarConfig.printLogMsgDelayMs);
    }

    public void boot() {
        try {
            ClientBuilder clientBuilder = PulsarClient.builder()
                    .operationTimeout(pulsarConfig.operationTimeoutSeconds, TimeUnit.SECONDS)
                    .ioThreads(pulsarConfig.ioThreads);
            if (pulsarConfig.tlsEnable) {
                Map<String, String> map = new HashMap<>();
                map.put("keyStoreType", "JKS");
                map.put("keyStorePath", pulsarConfig.keyStorePath);
                map.put("keyStorePassword", pulsarConfig.keyStorePassword);
                pulsarClient = clientBuilder.allowTlsInsecureConnection(true)
                        .serviceUrl(String.format("https://%s:%s", pulsarConfig.host, pulsarConfig.port))
                        .enableTlsHostnameVerification(false).useKeyStoreTls(true)
                        .tlsTrustStoreType("JKS")
                        .tlsTrustStorePath(pulsarConfig.tlsTrustStorePath)
                        .tlsTrustStorePassword(pulsarConfig.tlsTrustStorePassword)
                        .authentication(AUTH_PLUGIN_CLASS_NAME, map).build();
            } else {
                pulsarClient = clientBuilder.serviceUrl(String.format("http://%s:%s", pulsarConfig.host,
                        pulsarConfig.port)).build();
            }
        } catch (Exception e) {
            log.error("create pulsar client exception ", e);
            throw new IllegalArgumentException("build pulsar client exception, exit");
        }
        // now we have pulsar client, we start pulsar consumer
        startConsumers(getTopicList());
    }

    public void startConsumers(List<String> topics) {
        if (commonConfig.consumeMode.equals(ConsumeMode.LISTEN)) {
            startConsumersListen(topics);
        } else {
            startConsumersPull(topics);
        }
    }

    public void startConsumersListen(List<String> topics) {
        if (commonConfig.exchangeType.equals(ExchangeType.BYTES)) {
            startConsumersListenBytes(topics);
        } else if (commonConfig.exchangeType.equals(ExchangeType.BYTE_BUFFER)) {
            startConsumersListenByteBuffer(topics);
        } else if (commonConfig.exchangeType.equals(ExchangeType.STRING)) {
            startConsumersListenString(topics);
        }
    }

    public void startConsumersListenBytes(List<String> topics) {
        for (String topic : topics) {
            try {
                createConsumerBuilderBytes(topic)
                        .messageListener((MessageListener<byte[]>) (consumer, msg)
                                -> {
                            log.debug("do nothing {}", msg.getMessageId());
                            traceReporter.reportTrace(PulsarUtils.generateTraceBean(msg));
                            e2EMetricsBean.recodeE2ELatency(System.currentTimeMillis() - msg.getPublishTime(),
                                    msg.getTopicName(), msg.getMessageId().toString());
                            ActionMsg<byte[]> actionMsg = new ActionMsg<>();
                            actionMsg.setMessageId(msg.getMessageId().toString());
                            actionMsg.setContent(msg.getValue());
                            consumer.acknowledgeAsync(msg);
                            executor.execute(() -> actionService.handleBytesMsg(actionMsg));
                        }).subscribe();
            } catch (PulsarClientException e) {
                log.error("create consumer fail. topic [{}]", topic, e);
            }
        }
    }

    public void startConsumersListenByteBuffer(List<String> topics) {
        for (String topic : topics) {
            try {
                createConsumerBuilderByteBuffer(topic)
                        .messageListener((MessageListener<ByteBuffer>) (consumer, msg)
                                -> {
                            log.debug("do nothing {}", msg.getMessageId());
                            traceReporter.reportTrace(PulsarUtils.generateTraceBean(msg));
                            e2EMetricsBean.recodeE2ELatency(System.currentTimeMillis() - msg.getPublishTime(),
                                    msg.getTopicName(), msg.getMessageId().toString());
                            ActionMsg<ByteBuffer> actionMsg = new ActionMsg<>();
                            actionMsg.setMessageId(msg.getMessageId().toString());
                            actionMsg.setContent(msg.getValue());
                            consumer.acknowledgeAsync(msg);
                            executor.execute(() -> actionService.handleByteBufferMsg(actionMsg));
                        }).subscribe();
            } catch (PulsarClientException e) {
                log.error("create consumer fail. topic [{}]", topic, e);
            }
        }
    }

    public void startConsumersListenString(List<String> topics) {
        for (String topic : topics) {
            try {
                createConsumerBuilderBytes(topic)
                        .messageListener((MessageListener<byte[]>) (consumer, msg)
                                -> {
                            log.debug("do nothing {}", msg.getMessageId());
                            traceReporter.reportTrace(PulsarUtils.generateTraceBean(msg));
                            e2EMetricsBean.recodeE2ELatency(System.currentTimeMillis() - msg.getPublishTime(),
                                    msg.getTopicName(), msg.getMessageId().toString());
                            ActionMsg<String> actionMsg = new ActionMsg<>();
                            actionMsg.setMessageId(msg.getMessageId().toString());
                            actionMsg.setContent(new String(msg.getValue(), StandardCharsets.UTF_8));
                            consumer.acknowledgeAsync(msg);
                            executor.execute(() -> actionService.handleStrMsg(actionMsg));
                        }).subscribe();
            } catch (PulsarClientException e) {
                log.error("create consumer fail. topic [{}]", topic, e);
            }
        }
    }

    public void startConsumersPull(List<String> topics) {
        if (commonConfig.exchangeType.equals(ExchangeType.BYTES)) {
            startConsumersPullBytes(topics);
        } else if (commonConfig.exchangeType.equals(ExchangeType.BYTE_BUFFER)) {
            startConsumersPullByteBuffer(topics);
        } else if (commonConfig.exchangeType.equals(ExchangeType.STRING)) {
            startConsumersPullString(topics);
        }
    }

    public void startConsumersPullBytes(List<String> topics) {
        List<List<Consumer<byte[]>>> consumerListList = new ArrayList<>();
        List<Semaphore> semaphores = new ArrayList<>();
        for (int i = 0; i < commonConfig.pullThreads; i++) {
            consumerListList.add(new ArrayList<>());
        }
        int aux = 0;
        for (String topic : topics) {
            try {
                final Consumer<byte[]> consumer = createConsumerBuilderBytes(topic).subscribe();
                if (pulsarConfig.subscriptionSeekTimestamp != 0) {
                    consumer.seek(pulsarConfig.subscriptionSeekTimestamp);
                }
                int index = aux % commonConfig.pullThreads;
                consumerListList.get(index).add(consumer);
                if (pulsarConfig.receiveLimiter == -1) {
                    semaphores.add(null);
                } else {
                    semaphores.add(new Semaphore(pulsarConfig.receiveLimiter));
                }
                aux++;
            } catch (PulsarClientException e) {
                log.error("create consumer fail. topic [{}]", topic, e);
            }
        }
        for (int i = 0; i < commonConfig.pullThreads; i++) {
            log.info("start pulsar pull thread {}", i);
            new PulsarPullBytesThread(i, actionService, semaphores, consumerListList.get(i),
                    pulsarConfig, executor, e2EMetricsBean, traceReporter).start();
        }
    }

    public void startConsumersPullByteBuffer(List<String> topics) {
        List<List<Consumer<ByteBuffer>>> consumerListList = new ArrayList<>();
        List<Semaphore> semaphores = new ArrayList<>();
        for (int i = 0; i < commonConfig.pullThreads; i++) {
            consumerListList.add(new ArrayList<>());
        }
        int aux = 0;
        for (String topic : topics) {
            try {
                final Consumer<ByteBuffer> consumer = createConsumerBuilderByteBuffer(topic).subscribe();
                if (pulsarConfig.subscriptionSeekTimestamp != 0) {
                    consumer.seek(pulsarConfig.subscriptionSeekTimestamp);
                }
                int index = aux % commonConfig.pullThreads;
                consumerListList.get(index).add(consumer);
                if (pulsarConfig.receiveLimiter == -1) {
                    semaphores.add(null);
                } else {
                    semaphores.add(new Semaphore(pulsarConfig.receiveLimiter));
                }
                aux++;
            } catch (PulsarClientException e) {
                log.error("create consumer fail. topic [{}]", topic, e);
            }
        }
        for (int i = 0; i < commonConfig.pullThreads; i++) {
            log.info("start pulsar pull thread {}", i);
            new PulsarPullByteBufferThread(i, actionService, semaphores, consumerListList.get(i),
                    pulsarConfig, executor, e2EMetricsBean, traceReporter).start();
        }
    }

    public void startConsumersPullString(List<String> topics) {
        List<List<Consumer<byte[]>>> consumerListList = new ArrayList<>();
        List<Semaphore> semaphores = new ArrayList<>();
        for (int i = 0; i < commonConfig.pullThreads; i++) {
            consumerListList.add(new ArrayList<>());
        }
        int aux = 0;
        for (String topic : topics) {
            try {
                final Consumer<byte[]> consumer = createConsumerBuilderBytes(topic).subscribe();
                if (pulsarConfig.subscriptionSeekTimestamp != 0) {
                    consumer.seek(pulsarConfig.subscriptionSeekTimestamp);
                }
                int index = aux % commonConfig.pullThreads;
                consumerListList.get(index).add(consumer);
                if (pulsarConfig.receiveLimiter == -1) {
                    semaphores.add(null);
                } else {
                    semaphores.add(new Semaphore(pulsarConfig.receiveLimiter));
                }
                aux++;
            } catch (PulsarClientException e) {
                log.error("create consumer fail. topic [{}]", topic, e);
            }
        }
        for (int i = 0; i < commonConfig.pullThreads; i++) {
            log.info("start pulsar pull thread {}", i);
            new PulsarPullStringThread(i, actionService, semaphores, consumerListList.get(i),
                    pulsarConfig, executor, e2EMetricsBean, traceReporter).start();
        }
    }

    public ConsumerBuilder<byte[]> createConsumerBuilderBytes(String topic) {
        ConsumerBuilder<byte[]> builder = pulsarClient.newConsumer().topic(topic);
        builder = builder.subscriptionName(pulsarConfig.getSubscriptionName());
        builder = builder.subscriptionType(pulsarConfig.subscriptionType);
        if (pulsarConfig.autoUpdatePartition) {
            builder.autoUpdatePartitions(true);
            builder.autoUpdatePartitionsInterval(pulsarConfig.autoUpdatePartitionSeconds, TimeUnit.SECONDS);
        }
        if (pulsarConfig.enableAckTimeout) {
            builder.ackTimeout(pulsarConfig.ackTimeoutMilliseconds, TimeUnit.MILLISECONDS);
            builder.ackTimeoutTickTime(pulsarConfig.ackTimeoutTickTimeMilliseconds, TimeUnit.MILLISECONDS);
        }
        builder.subscriptionInitialPosition(pulsarConfig.subscriptionInitialPosition);
        builder.receiverQueueSize(pulsarConfig.receiveQueueSize);
        if (!pulsarConfig.consumeBatch) {
            return builder;
        }
        final BatchReceivePolicy batchReceivePolicy = BatchReceivePolicy.builder()
                .timeout(pulsarConfig.consumeBatchTimeoutMs, TimeUnit.MILLISECONDS)
                .maxNumMessages(pulsarConfig.consumeBatchMaxMessages).build();
        return builder.batchReceivePolicy(batchReceivePolicy);
    }

    public ConsumerBuilder<ByteBuffer> createConsumerBuilderByteBuffer(String topic) {
        ConsumerBuilder<ByteBuffer> builder = pulsarClient.newConsumer(Schema.BYTEBUFFER).topic(topic);
        builder = builder.subscriptionName(pulsarConfig.getSubscriptionName());
        builder = builder.subscriptionType(pulsarConfig.subscriptionType);
        if (pulsarConfig.autoUpdatePartition) {
            builder.autoUpdatePartitions(true);
            builder.autoUpdatePartitionsInterval(pulsarConfig.autoUpdatePartitionSeconds, TimeUnit.SECONDS);
        }
        if (pulsarConfig.enableAckTimeout) {
            builder.ackTimeout(pulsarConfig.ackTimeoutMilliseconds, TimeUnit.MILLISECONDS);
            builder.ackTimeoutTickTime(pulsarConfig.ackTimeoutTickTimeMilliseconds, TimeUnit.MILLISECONDS);
        }
        builder.subscriptionInitialPosition(pulsarConfig.subscriptionInitialPosition);
        builder.receiverQueueSize(pulsarConfig.receiveQueueSize);
        if (!pulsarConfig.consumeBatch) {
            return builder;
        }
        final BatchReceivePolicy batchReceivePolicy = BatchReceivePolicy.builder()
                .timeout(pulsarConfig.consumeBatchTimeoutMs, TimeUnit.MILLISECONDS)
                .maxNumMessages(pulsarConfig.consumeBatchMaxMessages).build();
        return builder.batchReceivePolicy(batchReceivePolicy);
    }

    private List<String> getTopicList() {
        List<String> topics = new ArrayList<>();
        log.info("tenant prefix name [{}].", pulsarConfig.tenantPrefix);
        if (!pulsarConfig.tenantPrefix.isBlank()) {
            if (pulsarConfig.namespacePrefix.isBlank()) {
                log.info("namespace prefix name is blank.");
                return topics;
            }
            List<String> namespaces = namespaces();
            if (pulsarConfig.tenantSuffixNum == 0) {
                String tenantName = pulsarConfig.tenantPrefix;
                topics = topics(tenantName, namespaces);
            } else {
                for (int i = 0; i < pulsarConfig.tenantSuffixNum; i++) {
                    String tenantName = NameUtil.name(pulsarConfig.tenantPrefix,
                            i, pulsarConfig.tenantSuffixNumOfDigits);
                    topics.addAll(topics(tenantName, namespaces));
                }
            }
        } else {
            if (pulsarConfig.topicSuffixNum == 0) {
                topics.add(PulsarUtils.topicFn(pulsarConfig.tenant, pulsarConfig.namespace, pulsarConfig.topic));
            } else {
                for (int i = 0; i < pulsarConfig.topicSuffixNum; i++) {
                    topics.add(PulsarUtils.topicFn(pulsarConfig.tenant, pulsarConfig.namespace,
                            pulsarConfig.topic + i));
                }
            }
        }
        return topics;
    }

    private List<String> namespaces() {
        List<String> namespaceNames = new ArrayList<>();
        if (pulsarConfig.namespaceSuffixNum == 0) {
            namespaceNames.add(pulsarConfig.namespacePrefix);
        }
        for (int i = 0; i < pulsarConfig.namespaceSuffixNum; i++) {
            String namespaceName = NameUtil.name(pulsarConfig.namespacePrefix, i
                    , pulsarConfig.namespaceSuffixNumOfDigits);
            namespaceNames.add(namespaceName);
        }
        return namespaceNames;
    }

    private List<String> topics(String tenantName, List<String> namespaceNames) {
        List<String> topics = new ArrayList<>();
        if (pulsarConfig.topicSuffixNum == 0) {
            for (String namespaceName : namespaceNames) {
                topics.add(PulsarUtils.topicFn(tenantName, namespaceName, pulsarConfig.topic));
            }
        } else {
            for (int i = 0; i < pulsarConfig.topicSuffixNum; i++) {
                String topicName = NameUtil.name(pulsarConfig.topic, i, pulsarConfig.topicSuffixNumOfDigits);
                for (String namespaceName : namespaceNames) {
                    topics.add(PulsarUtils.topicFn(tenantName, namespaceName, topicName));
                }
            }
        }
        return topics;
    }

}
