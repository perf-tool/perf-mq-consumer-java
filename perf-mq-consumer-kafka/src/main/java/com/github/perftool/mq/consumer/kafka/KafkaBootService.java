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

package com.github.perftool.mq.consumer.kafka;

import com.github.perftool.mq.consumer.common.config.CommonConfig;
import com.github.perftool.mq.consumer.common.module.ExchangeType;
import com.github.perftool.mq.consumer.common.service.ActionService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Slf4j
@Service
public class KafkaBootService {

    private final KafkaConfig kafkaConfig;

    private final CommonConfig commonConfig;

    private final ActionService actionService;

    public KafkaBootService(@Autowired KafkaConfig kafkaConfig, @Autowired CommonConfig commonConfig,
                            @Autowired ActionService actionService) {
        this.kafkaConfig = kafkaConfig;
        this.commonConfig = commonConfig;
        this.actionService = actionService;
    }

    public void boot() {
        List<String> topics = new ArrayList<>();
        if (kafkaConfig.topicSuffixNum == 0) {
            topics.add(kafkaConfig.topic);
        } else {
            for (int i = 0; i < kafkaConfig.topicSuffixNum; i++) {
                topics.add(kafkaConfig.topic + i);
            }
        }
        createConsumers(topics);
    }

    public void createConsumers(List<String> topics) {
        List<List<String>> strListList = new ArrayList<>();
        for (int i = 0; i < commonConfig.pullThreads; i++) {
            strListList.add(new ArrayList<>());
        }
        int aux = 0;
        for (String topic : topics) {
            int index = aux % commonConfig.pullThreads;
            strListList.get(index).add(topic);
            aux++;
        }
        for (int i = 0; i < commonConfig.pullThreads; i++) {
            log.info("start kafka pull thread {}", i);
            if (commonConfig.exchangeType.equals(ExchangeType.BYTES)) {
                new KafkaPullBytesThread(i, actionService, strListList.get(i), kafkaConfig).start();
            } else if (commonConfig.exchangeType.equals(ExchangeType.STRING)) {
                new KafkaPullStringThread(i, actionService, strListList.get(i), kafkaConfig).start();
            }
        }
    }

}
