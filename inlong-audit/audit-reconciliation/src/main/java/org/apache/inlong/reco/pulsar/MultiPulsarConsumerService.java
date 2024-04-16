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

import org.apache.inlong.reco.service.impl.ReconciliationCheckServiceImpl;

import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Service
public class MultiPulsarConsumerService implements AutoCloseable, InitializingBean {

    private static Logger LOGGER = LoggerFactory.getLogger(MultiPulsarConsumerService.class);

    @Value("${inlong.audit.reconciliation.pulsar.subscription}")
    private String subName;

    @Value("${inlong.audit.reconciliation.pulsar.topic}")
    private String topic;

    @Value("${inlong.audit.reconciliation.pulsar.url}")
    private String pulsarBrokerUrl;

    @Value("${inlong.audit.reconciliation.pulsar.receive.queue.size:1000}")
    private Integer receiveQueueSize;

    @Value("${inlong.audit.reconciliation.pulsar.connections.per.broker:1}")
    private Integer connectionsPerBroker = 1;

    @Value("${inlong.audit.reconciliation.pulsar.client.op.timeout:30}")
    private Integer pulsarClientOpTimeout = 30;

    @Value("${inlong.audit.reconciliation.pulsar.client.num:1}")
    private Integer pulsarClientNum = 1;

    @Value("${inlong.audit.reconciliation.pulsar.per.client.consumer.num:1}")
    private Integer consumerNumPerClient = 1;

    @Autowired
    private ReconciliationCheckServiceImpl reconciliationCheckService;

    private List<PulsarClient> pulsarClientList = new ArrayList<>();

    private List<PulsarConsumerService> consumerServiceList = new ArrayList<>();

    public void start() throws Exception {
        LOGGER.info("Audit reconciliation starting ,pulsar = {} ,topic = {}, subName = {}",
                pulsarBrokerUrl, topic, subName);
        if (StringUtils.isEmpty(topic) || StringUtils.isEmpty(subName)
                || StringUtils.isEmpty(pulsarBrokerUrl)) {
            String errorMsg = String.format("topic = %s, subName = %s, pulsarBrokerUrl = %s", topic,
                    subName,
                    pulsarBrokerUrl);
            LOGGER.error("config has error! error msg " + errorMsg);
            throw new Exception("config has error! error msg" + errorMsg);
        }

        for (int i = 0; i < pulsarClientNum; i++) {
            PulsarClient pulsarClient = createPulsarClient();
            if (pulsarClient != null) {
                pulsarClientList.add(pulsarClient);
                for (int j = 0; j < consumerNumPerClient; j++) {
                    Consumer<byte[]> consumer = createConsumer(pulsarClient);
                    if (consumer != null) {
                        PulsarConsumerService consumerService =
                                new PulsarConsumerService(consumer, reconciliationCheckService);
                        consumerServiceList.add(consumerService);
                        consumerService.start();
                    }
                }
            }
        }
        LOGGER.info("dbsync audit server started!");
    }

    private PulsarClient createPulsarClient() throws Exception {
        ClientBuilder clientBuilder = PulsarClient.builder()
                .serviceUrl(pulsarBrokerUrl)
                .connectionsPerBroker(connectionsPerBroker)
                .connectionTimeout(pulsarClientOpTimeout, TimeUnit.SECONDS);
        PulsarClient client = null;
        try {
            client = clientBuilder.build();
        } catch (PulsarClientException e) {
            LOGGER.error("create pulsar client has exception e = {}", e);
            throw e;
        }
        return client;
    }

    private Consumer<byte[]> createConsumer(PulsarClient client) throws Exception {
        ConsumerBuilder<byte[]> builder = client.newConsumer()
                .enableRetry(true)
                .topic(topic)
                .subscriptionName(subName)
                .receiverQueueSize(receiveQueueSize)
                .subscriptionType(SubscriptionType.Shared);
        Consumer<byte[]> consumer = null;
        try {
            consumer = builder.subscribe();
        } catch (PulsarClientException e) {
            LOGGER.error("create pulsar consumer has exception e = {}", e);
            throw e;
        }
        return consumer;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                close();
            } catch (Exception e) {
                LOGGER.error("stop has exception! e = {}", e);
            }
        }));
    }

    @Override
    public void close() throws Exception {
        if (consumerServiceList.size() > 0) {
            for (PulsarConsumerService consumerService : consumerServiceList) {
                consumerService.close();
            }
        }
        if (pulsarClientList.size() > 0) {
            for (PulsarClient pulsarClient : pulsarClientList) {
                pulsarClient.close();
            }
        }
    }
}
