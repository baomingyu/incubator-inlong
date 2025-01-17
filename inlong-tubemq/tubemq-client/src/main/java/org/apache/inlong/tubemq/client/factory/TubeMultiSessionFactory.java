/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.tubemq.client.factory;

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.inlong.tubemq.client.config.ConsumerConfig;
import org.apache.inlong.tubemq.client.config.TubeClientConfig;
import org.apache.inlong.tubemq.client.config.TubeClientConfigUtils;
import org.apache.inlong.tubemq.client.consumer.PullMessageConsumer;
import org.apache.inlong.tubemq.client.consumer.PushMessageConsumer;
import org.apache.inlong.tubemq.client.exception.TubeClientException;
import org.apache.inlong.tubemq.client.producer.MessageProducer;
import org.apache.inlong.tubemq.corebase.Shutdownable;
import org.apache.inlong.tubemq.corerpc.RpcConfig;
import org.apache.inlong.tubemq.corerpc.netty.NettyClientFactory;

public class TubeMultiSessionFactory implements MessageSessionFactory {

    private final NettyClientFactory clientFactory = new NettyClientFactory();
    private final TubeBaseSessionFactory baseSessionFactory;
    private final AtomicBoolean isShutDown = new AtomicBoolean(true);

    public TubeMultiSessionFactory(final TubeClientConfig tubeClientConfig) throws TubeClientException {
        RpcConfig config = TubeClientConfigUtils.getRpcConfigByClientConfig(tubeClientConfig, false);
        clientFactory.configure(config);
        baseSessionFactory = new TubeBaseSessionFactory(clientFactory, tubeClientConfig);
        isShutDown.set(false);
    }

    @Override
    public void shutdown() throws TubeClientException {
        if (isShutDown.compareAndSet(false, true)) {
            baseSessionFactory.shutdown();
            clientFactory.shutdown();
        }
    }

    @Override
    public <T extends Shutdownable> void removeClient(final T client) {
        if (baseSessionFactory == null) {
            return;
        }
        this.baseSessionFactory.removeClient(client);
    }

    @Override
    public MessageProducer createProducer() throws TubeClientException {
        if (isShutDown.get()) {
            throw new TubeClientException("Please initialize the object first!");
        }
        return this.baseSessionFactory.createProducer();
    }

    @Override
    public PushMessageConsumer createPushConsumer(final ConsumerConfig consumerConfig)
            throws TubeClientException {
        if (isShutDown.get()) {
            throw new TubeClientException("Please initialize the object first!");
        }
        return this.baseSessionFactory.createPushConsumer(consumerConfig);
    }

    @Override
    public PullMessageConsumer createPullConsumer(ConsumerConfig consumerConfig)
            throws TubeClientException {
        if (isShutDown.get()) {
            throw new TubeClientException("Please initialize the object first!");
        }
        return this.baseSessionFactory.createPullConsumer(consumerConfig);
    }

}
