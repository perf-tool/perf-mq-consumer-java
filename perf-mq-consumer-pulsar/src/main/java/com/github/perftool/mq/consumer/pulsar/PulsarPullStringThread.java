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
import com.github.perftool.mq.consumer.common.metrics.E2EMetricsBean;
import com.github.perftool.mq.consumer.common.service.ActionService;
import io.github.perftool.trace.report.ITraceReporter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Messages;
import org.jetbrains.annotations.NotNull;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;

@Slf4j
public class PulsarPullStringThread extends AbstractPulsarPullThread<byte[]> {


    public PulsarPullStringThread(int i,
                                  ActionService actionService,
                                  List<Semaphore> semaphores,
                                  List<Consumer<byte[]>> consumers,
                                  PulsarConfig pulsarConfig,
                                  ExecutorService executor,
                                  E2EMetricsBean e2EMetricsBean,
                                  ITraceReporter traceReporter
    ) {
        super(i, actionService, semaphores, consumers, pulsarConfig, executor, e2EMetricsBean, traceReporter);
    }

    protected void handleBatch(Messages<byte[]> messages) {
        final ArrayList<ActionMsg<String>> list = new ArrayList<>();
        for (Message<byte[]> message : messages) {
            e2EMetricsBean.recodeE2ELatency(System.currentTimeMillis() - message.getPublishTime(),
                    message.getTopicName(), message.getMessageId().toString());
            list.add(new ActionMsg<>(message.getMessageId().toString(),
                    new String(message.getValue(), StandardCharsets.UTF_8)));
        }
        this.actionService.handleStrBatchMsg(list);
    }

    protected void handle(@NotNull Message<byte[]> message) {
        this.actionService.handleStrMsg(new ActionMsg<>(message.getMessageId().toString(),
                                                   new String(message.getValue(), StandardCharsets.UTF_8)));
    }

}
