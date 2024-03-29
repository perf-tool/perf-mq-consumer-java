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

package com.github.perftool.mq.consumer.action;

import com.github.perftool.mq.consumer.action.module.ActionType;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import java.util.concurrent.TimeUnit;

public class ActionMetricsBean {

    private static final String COUNT_NAME = "action";

    private static final String SUCCESS_COUNT_NAME = "action_success";

    private static final String FAIL_COUNT_NAME = "action_fail";

    private static final String SUCCESS_LATENCY_SUMMARY_NAME = "action_success_latency_summary";

    private static final String FAIL_LATENCY_SUMMARY_NAME = "action_fail_latency_summary";

    private static final String SUCCESS_LATENCY_TIMER_NAME = "action_success_latency_timer";

    private static final String FAIL_LATENCY_TIMER_NAME = "action_fail_latency_timer";

    private final Counter counter;

    private final Counter successCounter;

    private final Counter failCounter;

    private final DistributionSummary successSummary;

    private final DistributionSummary failSummary;

    private final Timer successTimer;

    private final Timer failTimer;

    public ActionMetricsBean(MeterRegistry meterRegistry, ActionType actionType) {
        String[] tags = new String[]{"action_type", actionType.toString()};
        this.counter = meterRegistry.counter(COUNT_NAME, tags);
        this.successCounter = meterRegistry.counter(SUCCESS_COUNT_NAME, tags);
        this.failCounter = meterRegistry.counter(FAIL_COUNT_NAME, tags);
        this.successSummary = meterRegistry.summary(SUCCESS_LATENCY_SUMMARY_NAME, tags);
        this.failSummary = meterRegistry.summary(FAIL_LATENCY_SUMMARY_NAME, tags);
        this.successTimer = Timer.builder(SUCCESS_LATENCY_TIMER_NAME)
                .publishPercentiles(0.5, 0.75, 0.9, 0.95, 0.99, 0.999)
                .publishPercentileHistogram(true)
                .tags(tags).register(meterRegistry);
        this.failTimer = Timer.builder(FAIL_LATENCY_TIMER_NAME)
                .publishPercentiles(0.5, 0.75, 0.9, 0.95, 0.99, 0.999)
                .publishPercentileHistogram(true)
                .tags(tags).register(meterRegistry);
    }

    public void success(long cost) {
        counter.increment();
        successCounter.increment();
        successSummary.record(cost);
        successTimer.record(cost, TimeUnit.MILLISECONDS);
    }

    public void fail(long cost) {
        counter.increment();
        failCounter.increment();
        failSummary.record(cost);
        failTimer.record(cost, TimeUnit.MILLISECONDS);
    }

}
