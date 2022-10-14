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

package com.github.perftool.mq.consumer.common.trace.mongo;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

@Component
@Configuration
public class MongoDBConfig {

    @Value("${MONGODB_HOST:localhost}")
    public String mongodbHost;

    @Value("${MONGODB_PORT:27017}")
    public int mongodbPort;

    @Value("${MONGODB_USERNAME:}")
    public String mongodbUsername;

    @Value("${MONGODB_PASSWORD:}")
    public String mongodbPassword;

    @Value("${MONGODB_DATABASE_NAME:trace_database1}")
    public String mongodbDatabaseName;

    @Value("${MONGODB_COLLECT_NAME:trace_collect_consumer}")
    public String mongodbCollectionName;
}