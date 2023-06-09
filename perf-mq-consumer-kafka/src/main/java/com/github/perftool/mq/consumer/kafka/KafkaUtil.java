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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class KafkaUtil {
    public static boolean isRecordCorrupt(KafkaException e) {
        if (e.getMessage().contains(KafkaConst.ERROR_MSG_CORRUPT)) {
            return true;
        } else {
            if (e.getCause() instanceof KafkaException ke) {
                return isRecordCorrupt(ke);
            } else {
                return false;
            }
        }
    }
    public static Map<String, String> headers(ConsumerRecord<?, ?> record) {
        Headers headers = record.headers();
        Map<String, String> map = new HashMap<>();
        for (Header header : headers) {
            map.put(header.key(), new String(header.value(), StandardCharsets.UTF_8));
        }
        return map;
    }
}
