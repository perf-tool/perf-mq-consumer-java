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

package com.github.perftool.mq.consumer.action.pulsar;

import com.github.perftool.mq.consumer.action.IAction;
import com.github.perftool.mq.consumer.action.MsgCallback;
import com.github.perftool.mq.consumer.action.module.ActionMsg;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.SizeUnit;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@Slf4j
public class PulsarStrAction implements IAction<String> {
    private final ActionPulsarConfig config;

    private Producer<byte[]> producer;

    public PulsarStrAction(ActionPulsarConfig config) {
        this.config = config;
    }

    @Override
    public void init() {
        ClientBuilder clientBuilder = PulsarClient.builder()
                .memoryLimit(config.memoryLimitMb, SizeUnit.MEGA_BYTES)
                .ioThreads(config.pulsarIoThreads)
                .maxConcurrentLookupRequests(config.pulsarMaxConcurrentLookupRequests);
        try {
            PulsarClient pulsarClient = clientBuilder.serviceUrl(String.format("pulsar://%s", config.addr)).build();
            producer = pulsarClient.newProducer()
                    .maxPendingMessages(1000)
                    .enableBatching(config.enableBatching)
                    .batchingMaxBytes(config.batchingMaxBytes)
                    .batchingMaxMessages(config.batchingMaxMessages)
                    .batchingMaxPublishDelay(config.batchingMaxPublishDelay, TimeUnit.MILLISECONDS)
                    .topic(config.topic).create();
        } catch (Exception e) {
            log.error("build pulsar client failed", e);
        }
    }

    @Override
    public void handleBatchMsg(List<ActionMsg<String>> actionMsgs) {
        for (ActionMsg<String> actionMsg : actionMsgs) {
            this.handleMsg(actionMsg, Optional.empty());
        }
    }

    @Override
    public void handleMsg(ActionMsg<String> msg, Optional<MsgCallback> msgCallback) {
        try {
            CompletableFuture<MessageId> messageIdCompletableFuture = producer
                    .sendAsync(msg.getContent().getBytes(StandardCharsets.UTF_8));
            messageIdCompletableFuture.whenComplete((messageId, throwable) -> {
                if (throwable != null) {
                    log.error("error is ", throwable);
                } else {
                    log.info("original partition is [{}] message id is [{}] header is [{}] message id is [{}]",
                            msg.getPartition(), msg.getMessageId(), msg.getHeaders(), messageId);
                }
            });
        } catch (Exception e) {
            log.error("send req exception ", e);
        }
    }
}
