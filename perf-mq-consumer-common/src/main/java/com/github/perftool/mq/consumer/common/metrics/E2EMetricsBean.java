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

package com.github.perftool.mq.consumer.common.metrics;

import com.google.common.util.concurrent.RateLimiter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.TimeUnit;

@Slf4j
public class E2EMetricsBean {

    private final RateLimiter limiter = RateLimiter.create(100);

    private static final String E2E_LATENCY_TIMER_NAME = "perf_consumer_e2e_latency";

    private final Timer e2eLatency;

    private final int msgDelayMs;

    public E2EMetricsBean(MeterRegistry meterRegistry, String middleware, int msgDelayMs) {
        this.e2eLatency = Timer.builder(E2E_LATENCY_TIMER_NAME)
                .tags("middleware", middleware)
                .publishPercentiles(0.5, 0.75, 0.9, 0.95, 0.99, 0.999)
                .publishPercentileHistogram(false)
                .register(meterRegistry);
        this.msgDelayMs = msgDelayMs;
    }


    public void recodeE2ELatency(long cost, String topic, String msgId) {
        if (limiter.tryAcquire() && cost > msgDelayMs) {
            log.info("{} msg {} cost time {}", topic, msgId, cost);
        }
        e2eLatency.record(cost, TimeUnit.MILLISECONDS);
    }

}
