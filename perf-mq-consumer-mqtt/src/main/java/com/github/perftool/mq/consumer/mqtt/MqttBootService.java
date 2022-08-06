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

package com.github.perftool.mq.consumer.mqtt;

import com.github.perftool.mq.consumer.action.module.ActionMsg;
import com.github.perftool.mq.consumer.common.config.CommonConfig;
import com.github.perftool.mq.consumer.common.module.ExchangeType;
import com.github.perftool.mq.consumer.common.service.ActionService;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;

@Slf4j
@Service
public class MqttBootService implements MqttCallback {

    private final MqttConfig mqttConfig;

    private final CommonConfig commonConfig;

    private final ActionService actionService;

    public MqttBootService(@Autowired MqttConfig mqttConfig, @Autowired CommonConfig commonConfig,
                           @Autowired ActionService actionService) {
        this.mqttConfig = mqttConfig;
        this.commonConfig = commonConfig;
        this.actionService = actionService;
    }

    public void boot() throws Exception {
        MqttClient mqttClient = new MqttClient(String.format("tcp://%s:%d", mqttConfig.host, mqttConfig.port),
                mqttConfig.clientId);
        mqttClient.setCallback(this);
        MqttConnectOptions mqttConnectOptions = new MqttConnectOptions();
        mqttConnectOptions.setUserName(mqttConfig.username);
        mqttConnectOptions.setPassword(mqttConfig.password.toCharArray());
        mqttClient.connect(mqttConnectOptions);
        mqttClient.subscribe(mqttConfig.topic);
        mqttClient.setCallback(this);
    }

    @Override
    public void connectionLost(Throwable throwable) {

    }

    @Override
    public void messageArrived(String topic, MqttMessage message) throws Exception {
        if (commonConfig.exchangeType.equals(ExchangeType.STRING)) {
            ActionMsg<String> actionMsg = new ActionMsg<>();
            actionMsg.setContent(new String(message.getPayload(), StandardCharsets.UTF_8));
            this.actionService.handleStrMsg(actionMsg);
        } else if (commonConfig.exchangeType.equals(ExchangeType.BYTES)) {
            ActionMsg<byte[]> actionMsg = new ActionMsg<>();
            actionMsg.setContent(message.getPayload());
            this.actionService.handleBytesMsg(actionMsg);
        } else {
            log.error("mqtt doesn't support byte buffer yet.");
        }
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {

    }
}
