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

package com.github.perftool.mq.consumer.common.service;

import com.github.com.perftoo.mq.consumer.action.empty.EmptyStrAction;
import com.github.perftool.mq.consumer.action.kafka.ActionKafkaConfig;
import com.github.perftool.mq.consumer.action.kafka.KafkaBytesAction;
import com.github.perftool.mq.consumer.action.kafka.KafkaStrAction;
import com.github.perftool.mq.consumer.action.pulsar.ActionPulsarConfig;
import com.github.perftool.mq.consumer.action.pulsar.PulsarAction;
import com.github.perftool.mq.consumer.common.module.ExchangeType;
import com.github.perftool.mq.consumer.action.IAction;
import com.github.perftool.mq.consumer.action.MsgCallback;
import com.github.perftool.mq.consumer.action.http.common.ActionHttpConfig;
import com.github.perftool.mq.consumer.action.http.okhttp.OkhttpStrAction;
import com.github.perftool.mq.consumer.action.influx.ActionInfluxConfig;
import com.github.perftool.mq.consumer.action.influx.InfluxStrAction;
import com.github.perftool.mq.consumer.action.influx1.ActionInflux1Config;
import com.github.perftool.mq.consumer.action.influx1.Influx1StrAction;
import com.github.perftool.mq.consumer.action.kafka.KafkaByteBufferAction;
import com.github.perftool.mq.consumer.action.log.ActionLogConfig;
import com.github.perftool.mq.consumer.action.log.LogStrAction;
import com.github.perftool.mq.consumer.action.module.ActionMsg;
import com.github.perftool.mq.consumer.common.config.ActionConfig;
import com.github.perftool.mq.consumer.common.config.CommonConfig;
import com.github.perftool.mq.consumer.action.module.ActionType;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
public class ActionService {

    @Autowired
    private ActionConfig actionConfig;

    @Autowired
    private ActionInfluxConfig actionInfluxConfig;

    @Autowired
    private ActionInflux1Config actionInflux1Config;

    @Autowired
    private ActionKafkaConfig actionKafkaConfig;

    @Autowired
    private ActionLogConfig actionLogConfig;

    @Autowired
    private CommonConfig commonConfig;

    @Autowired
    private ActionHttpConfig actionHttpConfig;

    @Autowired
    private ActionPulsarConfig actionPulsarConfig;

    @Autowired
    private MeterRegistry meterRegistry;

    private Optional<IAction<ByteBuffer>> byteBufferAction = Optional.empty();

    private Optional<IAction<byte[]>> bytesAction = Optional.empty();

    private Optional<IAction<String>> strAction = Optional.empty();

    private final Optional<MsgCallback> msgCallback = Optional.empty();

    @PostConstruct
    public void init() {
        if (commonConfig.exchangeType.equals(ExchangeType.BYTE_BUFFER)) {
            if (actionConfig.actionType.equals(ActionType.KAFKA)) {
                byteBufferAction = Optional.of(new KafkaByteBufferAction(actionKafkaConfig, meterRegistry));
            }
        }
        if (commonConfig.exchangeType.equals(ExchangeType.BYTES)) {
            if (actionConfig.actionType.equals(ActionType.KAFKA)) {
                bytesAction = Optional.of(new KafkaBytesAction(actionKafkaConfig, meterRegistry));
            }
            if (actionConfig.actionType.equals(ActionType.PULSAR)) {
                bytesAction = Optional.of(new PulsarAction(actionPulsarConfig));
            }
        }
        if (commonConfig.exchangeType.equals(ExchangeType.STRING)) {
            switch (actionConfig.actionType) {
                case INFLUX -> strAction = Optional.of(new InfluxStrAction());
                case INFLUX1 -> strAction = Optional.of(new Influx1StrAction());
                case KAFKA -> strAction = Optional.of(new KafkaStrAction(actionKafkaConfig, meterRegistry));
                case LOG -> strAction = Optional.of(new LogStrAction(actionLogConfig));
                case OKHTTP -> strAction = Optional.of(new OkhttpStrAction(actionHttpConfig));
                default -> strAction = Optional.of(new EmptyStrAction());
            }
        }
        byteBufferAction.ifPresent(IAction::init);
        bytesAction.ifPresent(IAction::init);
        strAction.ifPresent(IAction::init);
    }

    public void handleStrBatchMsg(List<ActionMsg<String>> msgList) {
        blockIfNeeded();
        strAction.ifPresent(action -> action.handleBatchMsg(msgList));
    }

    public void handleStrMsg(@NotNull ActionMsg<String> msg) {
        blockIfNeeded();
        strAction.ifPresent(action -> action.handleMsg(msg, msgCallback));
    }

    public void handleBytesBatchMsg(List<ActionMsg<byte[]>> msgList) {
        blockIfNeeded();
        bytesAction.ifPresent(action -> action.handleBatchMsg(msgList));
    }

    public void handleBytesMsg(@NotNull ActionMsg<byte[]> msg) {
        blockIfNeeded();
        bytesAction.ifPresent(action -> action.handleMsg(msg, msgCallback));
    }

    public void handleByteBufferBatchMsg(List<ActionMsg<ByteBuffer>> msgList) {
        blockIfNeeded();
        byteBufferAction.ifPresent(action -> action.handleBatchMsg(msgList));
    }

    public void handleByteBufferMsg(@NotNull ActionMsg<ByteBuffer> msg) {
        blockIfNeeded();
        byteBufferAction.ifPresent(action -> action.handleMsg(msg, msgCallback));
    }

    private void blockIfNeeded() {
        if (actionConfig.actionBlockDelayMs != 0) {
            try {
                TimeUnit.MILLISECONDS.sleep(actionConfig.actionBlockDelayMs);
            } catch (InterruptedException ignored) {
            }
        }
    }

}
