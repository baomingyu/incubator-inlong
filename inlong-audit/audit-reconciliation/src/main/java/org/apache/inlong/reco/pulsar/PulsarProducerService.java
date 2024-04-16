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

import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class PulsarProducerService {

    private static Logger LOGGER = LoggerFactory.getLogger(PulsarProducerService.class);

    private String pulsarBrokerUrl;

    private String pulsarTopic;

    private Producer<byte[]> producer;

    public PulsarProducerService(String pulsarBrokerUrl, String pulsarTopic) {
        this.pulsarTopic = pulsarTopic;
        this.pulsarBrokerUrl = pulsarBrokerUrl;
        this.producer = creatPulsarProducer();
    }

    public String sendNotifyMsg(byte[] msgBody) {
        if (producer == null) {
            producer = creatPulsarProducer();
        }
        if (producer != null) {

            try {
                MessageId messageId = producer.send(msgBody);
                if (messageId != null) {
                    return messageId.toString();
                }
            } catch (Exception e) {
                LOGGER.error("SendNotifyMsg has exception e = {}", e);
            }
        }
        return "";
    }

    private Producer<byte[]> creatPulsarProducer() {
        ClientBuilder clientBuilder = PulsarClient.builder()
                .serviceUrl(pulsarBrokerUrl)
                .connectionTimeout(60, TimeUnit.SECONDS);
        PulsarClient client = null;
        try {
            client = clientBuilder.build();
        } catch (PulsarClientException e) {
            LOGGER.error("create pulsar client has exception e = {}", e);
        }
        if (client != null) {
            try {
                return client.newProducer().topic(pulsarTopic).create();
            } catch (PulsarClientException e) {
                LOGGER.error("create pulsar producer has exception e = {}", e);
            }
        }
        return null;
    }
}
