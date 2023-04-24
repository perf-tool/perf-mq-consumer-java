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

package com.github.perftool.mq.consumer.common.config;

import com.github.perftool.mq.consumer.common.module.ExchangeType;
import com.github.perftool.mq.consumer.common.module.ConsumeMode;
import com.github.perftool.mq.consumer.common.module.TraceType;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Service;

@Configuration
@Service
public class CommonConfig {

    @Value("${CONSUME_MODE:PULL}")
    public ConsumeMode consumeMode;

    @Value("${EXCHANGE_TYPE:STRING}")
    public ExchangeType exchangeType;

    @Value("${PULL_THREADS:1}")
    public int pullThreads;

    @Value("${CONSUMER_TRACE_TYPE:DUMMY}")
    public TraceType traceType;

}
