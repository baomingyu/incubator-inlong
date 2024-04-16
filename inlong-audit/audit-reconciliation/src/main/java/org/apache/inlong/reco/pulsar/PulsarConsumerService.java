/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.reco.pulsar;

import org.apache.inlong.reco.pojo.RecoTriggerMsg;
import org.apache.inlong.reco.service.ReconciliationCheckService;

import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.shade.com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.concurrent.TimeUnit;

public class PulsarConsumerService extends Thread implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(PulsarConsumerService.class);

    private Consumer<byte[]> consumer;

    private volatile boolean isRunning = true;

    private Gson gson = new Gson();

    private ReconciliationCheckService reconciliationService;

    public PulsarConsumerService(Consumer<byte[]> consumer, ReconciliationCheckService reconciliationService) {
        this.consumer = consumer;
        this.reconciliationService = reconciliationService;
    }

    public void run() {
        int handleMsgCount = 0;
        long nowTime = Instant.now().toEpochMilli();
        while (isRunning) {
            Message<byte[]> msg = null;
            try {
                msg = consumer.receive(1, TimeUnit.SECONDS);
                if (msg != null) {
                    String body = null;
                    if (msg.getData() == null) {
                        consumer.acknowledge(msg);
                        LOGGER.warn("msg [{}] body is null!!!", msg.getMessageId());
                        continue;
                    }
                    try {
                        body = new String(msg.getData(), "UTF-8");
                        if (StringUtils.isNotEmpty(body)) {
                            RecoTriggerMsg recoTriggerMsg = gson.fromJson(body, RecoTriggerMsg.class);
                            if (reconciliationService.offerMsg(recoTriggerMsg, msg.getMessageId().toString())) {
                                handleMsgCount++;
                                consumer.acknowledge(msg);
                            } else {
                                consumer.negativeAcknowledge(msg);
                                LOGGER.warn("consumer msg offer msg is failure! {}, and it will be consumed later!",
                                        msg.getMessageId());
                            }
                        } else {
                            LOGGER.warn("consumer msg is empty! msgId = [{}]", msg.getMessageId());
                        }
                    } catch (Throwable e) {
                        LOGGER.error("consumer msg has exception!, topic = {}, msgId = {},"
                                + " body = {}, e:", msg.getTopicName(), msg.getMessageId(), body, e);
                        consumer.negativeAcknowledge(msg);
                    }
                }
                long handleTime = Instant.now().toEpochMilli();
                if ((handleTime - nowTime) > (1 * 60 * 1000L)) {
                    LOGGER.info("handle message msgCount = {}", handleMsgCount);
                    handleMsgCount = 0;
                    nowTime = handleTime;
                }
            } catch (PulsarClientException e) {
                LOGGER.error("PulsarClientException e = {}", e);
            }
        }
        try {
            consumer.close();
        } catch (PulsarClientException e) {
            LOGGER.error("close consumer has exception!, e ={} ", e);
        }
    }

    public void close() {
        isRunning = false;
    }
}
