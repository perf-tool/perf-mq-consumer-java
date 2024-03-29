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

package com.github.perftool.mq.consumer.common.util;

import com.github.perftool.mq.consumer.common.config.ThreadPoolConfig;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;


@Service
public class ThreadPool {

    @Autowired
    private ThreadPoolConfig config;

    public ExecutorService create(String name) {
        return new ThreadPoolExecutor(
                config.actionThreads,
                config.actionThreads,
                5,
                TimeUnit.MINUTES,
                new LinkedBlockingQueue<>(config.actionThreadPoolQueueSize),
                new DefaultThreadFactory(name),
                new ThreadPoolExecutor.CallerRunsPolicy()
        );
    }
}
