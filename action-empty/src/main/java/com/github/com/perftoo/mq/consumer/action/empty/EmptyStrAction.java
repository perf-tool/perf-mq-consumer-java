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

package com.github.com.perftoo.mq.consumer.action.empty;

import com.github.perftool.mq.consumer.action.IAction;
import com.github.perftool.mq.consumer.action.MsgCallback;
import com.github.perftool.mq.consumer.action.module.ActionMsg;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Optional;

@Slf4j
public class EmptyStrAction implements IAction<String> {

    public EmptyStrAction() {}

    @Override
    public void init() {

    }

    @Override
    public void handleBatchMsg(List<ActionMsg<String>> actionMsgs) {
    }

    @Override
    public void handleMsg(ActionMsg<String> msg, Optional<MsgCallback> msgCallback) {
    }

}
