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

package com.github.perftool.mq.consumer.common;

import com.github.perftool.mq.consumer.common.service.ActionService;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractPullThread extends Thread {

    protected final ActionService actionService;

    public AbstractPullThread(int i, ActionService actionService) {
        setName("pull- " + i);
        this.actionService = actionService;
    }

    @Override
    public void run() {
        while (true) {
            try {
                pull();
            } catch (Exception e) {
                log.error("ignore exception ", e);
            }
        }
    }

    protected abstract void pull() throws Exception;

}
