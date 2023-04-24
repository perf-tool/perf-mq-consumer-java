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

import com.github.perftool.mq.consumer.action.module.ActionMsg;
import com.github.perftool.mq.consumer.common.service.ActionService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.BytesDeserializer;

import java.util.List;

public class KafkaPullBytesThread extends AbstractKafkaPullThread<byte[]> {

    public KafkaPullBytesThread(int i, ActionService actionService, List<String> topics, KafkaConfig kafkaConfig) {
        super(i, actionService, topics, kafkaConfig);
    }

    @Override
    protected String getKeyDeserializerName() {
        return BytesDeserializer.class.getName();
    }

    @Override
    protected String getValueDeserializerName() {
        return BytesDeserializer.class.getName();
    }

    @Override
    protected void handle(ConsumerRecord<byte[], byte[]> record) {
        ActionMsg<byte[]> actionMsg = new ActionMsg<>();
        actionMsg.setMessageId(String.valueOf(record.offset()));
        actionMsg.setPartition(record.partition());
        actionMsg.setHeaders(KafkaUtil.headers(record));
        actionMsg.setContent(record.value());
        actionService.handleBytesMsg(actionMsg);
    }
}
