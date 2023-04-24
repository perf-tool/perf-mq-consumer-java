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

package com.github.perftool.mq.consumer.action.http.okhttp;

import com.github.perftool.mq.consumer.action.MsgCallback;
import com.github.perftool.mq.consumer.action.IAction;
import com.github.perftool.mq.consumer.action.http.common.ActionHttpConfig;
import com.github.perftool.mq.consumer.action.module.ActionMsg;
import lombok.extern.slf4j.Slf4j;
import okhttp3.Call;
import okhttp3.Callback;
import okhttp3.ConnectionPool;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Slf4j
public class OkhttpStrAction implements IAction<String> {

    private final ActionHttpConfig config;

    private final OkHttpClient client;

    private Request.Builder builder;

    public static final MediaType MEDIA_TYPE_JSON = MediaType.parse("application/json; charset=utf-8");

    public OkhttpStrAction(ActionHttpConfig config) {
        this.config = config;
        this.client = new OkHttpClient.Builder()
                .callTimeout(config.requestTimeoutSeconds, TimeUnit.SECONDS)
                .connectTimeout(config.connectionTimeoutSeconds, TimeUnit.SECONDS)
                .connectionPool(new ConnectionPool(config.maxIdleConnections,
                        config.connectionKeepaliveMinutes,
                        TimeUnit.MINUTES))
                .build();
    }

    @Override
    public void init() {
        String requestUrl = String.format("http://%s:%s/%s", config.host, config.port, config.uri);
        log.info("Starting OkHttp Server Action, remote {}", requestUrl);
        builder = new Request.Builder().url(requestUrl);
    }

    @Override
    public void handleBatchMsg(List<ActionMsg<String>> actionMsgs) {
        String content = actionMsgs.stream()
                .map(ActionMsg::getContent)
                .collect(Collectors.joining(",", "{", "}"));
        Request request = builder.post(RequestBody.create(content, MEDIA_TYPE_JSON)).build();
        sendMsgToHttpServer(request, Optional.empty(), Optional.empty());
    }

    @Override
    public void handleMsg(ActionMsg<String> msg, Optional<MsgCallback> msgCallback) {
        Request request = builder.post(RequestBody.create(msg.getContent(), MEDIA_TYPE_JSON)).build();
        sendMsgToHttpServer(request, Optional.ofNullable(msg.getMessageId()), msgCallback);
    }

    private void sendMsgToHttpServer(Request request, Optional<String> msgId, Optional<MsgCallback> msgCallback) {
        if (config.async) {
            client.newCall(request).enqueue(new Callback() {
                @Override
                public void onFailure(@NotNull Call call, @NotNull IOException e) {
                    log.error("send msg to http server {} failed", request.url(), e);
                    msgId.ifPresent(id -> msgCallback.ifPresent(callback -> callback.fail(id)));
                }

                @Override
                public void onResponse(@NotNull Call call, @NotNull Response response) throws IOException {
                    try (ResponseBody responseBody = response.body()) {
                        if (!response.isSuccessful()) {
                            if (responseBody != null) {
                                log.error("send msg to http server {} failed, response code: {}, reason: {}",
                                        request.url(), response.code(), responseBody.string());
                            } else {
                                log.error("send msg to http server {} failed, response code: {}",
                                        request.url(), response.code());
                            }
                            msgId.ifPresent(id -> msgCallback.ifPresent(callback -> callback.fail(id)));
                            return;
                        }
                        msgId.ifPresent(id -> msgCallback.ifPresent(callback -> callback.success(id)));
                    }
                }
            });
        } else {
            try (Response response = client.newCall(request).execute()) {
                if (!response.isSuccessful()) {
                    log.error("send msg to http server {} failed, response code: {}", request.url(), response.code());
                    msgId.ifPresent(id -> msgCallback.ifPresent(callback -> callback.fail(id)));
                    return;
                }
                msgId.ifPresent(id -> msgCallback.ifPresent(callback -> callback.success(id)));
            } catch (Exception e) {
                log.error("send msg to http server {} failed", request.url(), e);
                msgId.ifPresent(id -> msgCallback.ifPresent(callback -> callback.fail(id)));
            }
        }
    }

}
