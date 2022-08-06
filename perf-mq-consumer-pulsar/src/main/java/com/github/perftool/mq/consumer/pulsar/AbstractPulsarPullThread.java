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

import com.github.perftool.mq.consumer.common.AbstractPullThread;
import com.github.perftool.mq.consumer.common.service.ActionService;
import com.google.common.util.concurrent.RateLimiter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Messages;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

@Slf4j
public abstract class AbstractPulsarPullThread<T> extends AbstractPullThread {

    private final List<Consumer<T>> consumers;

    private final PulsarConfig pulsarConfig;

    private final RateLimiter rateLimiter;

    private final List<Semaphore> semaphores;

    private ExecutorService executor;

    public AbstractPulsarPullThread(int i, ActionService actionService, List<Semaphore> semaphores,
                                    List<Consumer<T>> consumers, PulsarConfig pulsarConfig, ExecutorService executor) {
        super(i, actionService);
        this.semaphores = semaphores;
        this.consumers = consumers;
        this.pulsarConfig = pulsarConfig;
        this.rateLimiter = pulsarConfig.rateLimiter == -1 ? null : RateLimiter.create(pulsarConfig.rateLimiter);
        this.executor = executor;
    }

    @Override
    protected void pull() throws Exception {
        if (rateLimiter != null && !rateLimiter.tryAcquire(5, TimeUnit.MILLISECONDS)) {
            return;
        }
        if (pulsarConfig.consumeAsync) {
            asyncReceive();
        } else {
            syncReceive();
        }
    }

    private void asyncReceive() throws Exception {
        for (int i = 0; i < consumers.size(); i++) {
            Consumer<T> consumer = consumers.get(i);
            Semaphore semaphore = semaphores.get(i);
            asyncReceiveConsumer(consumer, semaphore);
        }
    }

    private void asyncReceiveConsumer(Consumer<T> consumer, Semaphore semaphore) {
        if (semaphore != null && !semaphore.tryAcquire()) {
            return;
        }
        if (pulsarConfig.consumeBatch) {
            consumer.batchReceiveAsync().thenAcceptAsync(messages -> {
                        executor.execute(() -> handleBatch(messages));
                        consumer.acknowledgeAsync(messages);
                        if (semaphore != null) {
                            semaphore.release();
                        }
                    }
            ).exceptionally(ex -> {
                if (semaphore != null) {
                    semaphore.release();
                }
                log.error("batch receive error ", ex);
                return null;
            });
        } else {
            consumer.receiveAsync().thenAcceptAsync(message -> {
                executor.execute(() -> handle(message));
                consumer.acknowledgeAsync(message);
                if (semaphore != null) {
                    semaphore.release();
                }
            }).exceptionally(ex -> {
                if (semaphore != null) {
                    semaphore.release();
                }
                log.error("receive error ", ex);
                return null;
            });
        }
    }

    private void syncReceive() throws Exception {
        for (Consumer<T> consumer : consumers) {
            if (pulsarConfig.consumeBatch) {
                final Messages<T> messages = consumer.batchReceive();
                executor.execute(() -> handleBatch(messages));
                consumer.acknowledge(messages);
            } else {
                final Message<T> message =
                        consumer.receive(pulsarConfig.syncReceiveTimeoutMs, TimeUnit.MILLISECONDS);
                if (message == null) {
                    continue;
                }
                executor.execute(() -> handle(message));
                consumer.acknowledge(message);
            }
        }
    }

    protected abstract void handleBatch(Messages<T> messages);

    protected abstract void handle(@NotNull Message<T> message);

}
