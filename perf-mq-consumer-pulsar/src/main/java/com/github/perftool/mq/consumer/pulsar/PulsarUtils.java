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

package com.github.perftool.mq.consumer.pulsar;

import io.github.perftool.trace.module.SpanInfo;
import io.github.perftool.trace.module.TraceBean;
import io.github.perftool.trace.report.ReportUtil;
import io.github.perftool.trace.util.InboundCounter;
import io.github.perftool.trace.util.JacksonUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.Message;

public class PulsarUtils {

    private static final InboundCounter inboundCounter = new InboundCounter(999);

    public static String topicFn(String tenant, String namespace, String topic) {
        return String.format("persistent://%s/%s/%s", tenant, namespace, topic);
    }

    public static <T> TraceBean generateTraceBean(Message<T> msg) {
        String jsonStr = msg.getProperty("traceId");
        if (StringUtils.isEmpty(jsonStr)) {
            return null;
        }
        TraceBean traceBean = JacksonUtil.toObject(jsonStr, TraceBean.class);
        String spanId = String.format("%s-%d", ReportUtil.traceIdPrefix(), inboundCounter.get());
        SpanInfo spanInfo = traceBean.getSpanInfo();
        spanInfo.setSpanId(spanId);
        traceBean.setSpanInfo(spanInfo);
        return traceBean;
    }

}
