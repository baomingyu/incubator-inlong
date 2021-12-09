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

package org.apache.inlong.dataproxy.source;

import java.lang.reflect.Constructor;
import java.util.concurrent.TimeUnit;

import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.source.AbstractSource;
import org.apache.inlong.dataproxy.consts.ConfigConstants;
import org.apache.inlong.dataproxy.utils.MonitorIndex;
import org.apache.inlong.dataproxy.utils.MonitorIndexExt;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.handler.codec.frame.LengthFieldBasedFrameDecoder;
import org.jboss.netty.handler.execution.ExecutionHandler;
import org.jboss.netty.handler.execution.OrderedMemoryAwareThreadPoolExecutor;
import org.jboss.netty.handler.timeout.ReadTimeoutHandler;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServerMessageFactory implements ChannelPipelineFactory {

    private static final Logger LOG = LoggerFactory.getLogger(ServerMessageFactory.class);

    private static final int DEFAULT_READ_IDLE_TIME = 70 * 60 * 1000;

    private static long MAX_CHANNEL_MEMORY_SIZE = 1024 * 1024;

    private static long MAX_TOTAL_MEMORY_SIZE = 1024 * 1024;

    private static int MSG_LENGTH_LEN = 4;

    private AbstractSource source;

    private ChannelProcessor processor;

    private ChannelGroup allChannels;

    private ExecutionHandler executionHandler;

    private String protocolType;

    private ServiceDecoder serviceDecoder;

    private String messageHandlerName;

    private int maxConnections = Integer.MAX_VALUE;

    private int maxMsgLength;

    private boolean isCompressed;

    private String name;

    private String topic;

    private String attr;

    private boolean filterEmptyMsg;

    private MonitorIndex monitorIndex;

    private MonitorIndexExt monitorIndexExt;

    private Timer timer = new HashedWheelTimer();

    /**
     * get server factory
     *
     * @param source
     * @param allChannels
     * @param protocol
     * @param serviceDecoder
     * @param messageHandlerName
     * @param topic
     * @param attr
     * @param filterEmptyMsg
     * @param maxCons
     * @param isCompressed
     * @param monitorIndex
     * @param monitorIndexExt
     * @param name
     */
    public ServerMessageFactory(AbstractSource source,
                                ChannelGroup allChannels, String protocol, ServiceDecoder serviceDecoder,
                                String messageHandlerName, int maxMsgLength,
                                String topic, String attr, Boolean filterEmptyMsg, Integer maxCons,
                                Boolean isCompressed, MonitorIndex monitorIndex, MonitorIndexExt monitorIndexExt,
            String name) {
        this.source = source;
        this.processor = source.getChannelProcessor();
        this.allChannels = allChannels;
        this.topic = topic;
        this.attr = attr;
        this.filterEmptyMsg = filterEmptyMsg;
        int cores = Runtime.getRuntime().availableProcessors();
        this.protocolType = protocol;
        this.serviceDecoder = serviceDecoder;
        this.messageHandlerName = messageHandlerName;
        this.name = name;
        this.maxConnections = maxCons;
        this.maxMsgLength = maxMsgLength;
        this.isCompressed = isCompressed;
        this.monitorIndex = monitorIndex;
        this.monitorIndexExt = monitorIndexExt;
        if (protocolType.equalsIgnoreCase(ConfigConstants.UDP_PROTOCOL)) {
            this.executionHandler = new ExecutionHandler(
                    new OrderedMemoryAwareThreadPoolExecutor(cores * 2,
                            MAX_CHANNEL_MEMORY_SIZE, MAX_TOTAL_MEMORY_SIZE));
        }
    }

    @Override
    public ChannelPipeline getPipeline() throws Exception {
        ChannelPipeline cp = Channels.pipeline();
        return addMessageHandlersTo(cp);
    }

    /**
     * get message handlers
     * @param cp
     * @return
     */
    public ChannelPipeline addMessageHandlersTo(ChannelPipeline cp) {

        if (this.protocolType
                .equalsIgnoreCase(ConfigConstants.TCP_PROTOCOL)) {
            cp.addLast("messageDecoder", new LengthFieldBasedFrameDecoder(
                    this.maxMsgLength, 0, MSG_LENGTH_LEN, 0, 0, true));
            cp.addLast("readTimeoutHandler", new ReadTimeoutHandler(timer,
                    DEFAULT_READ_IDLE_TIME, TimeUnit.MILLISECONDS));
        }

        if (processor != null) {
            try {
                Class<? extends SimpleChannelHandler> clazz = (Class<? extends SimpleChannelHandler>) Class
                        .forName(messageHandlerName);

                Constructor<?> ctor = clazz.getConstructor(
                        AbstractSource.class, ServiceDecoder.class, ChannelGroup.class,
                        String.class, String.class, Boolean.class,
                        Integer.class, Boolean.class, MonitorIndex.class,
                        MonitorIndexExt.class, String.class);

                SimpleChannelHandler messageHandler = (SimpleChannelHandler) ctor
                        .newInstance(source, serviceDecoder, allChannels, topic, attr,
                                filterEmptyMsg, maxConnections,
                                isCompressed,  monitorIndex, monitorIndexExt, protocolType
                        );

                cp.addLast("messageHandler", messageHandler);
            } catch (Exception e) {
                LOG.info("SimpleChannelHandler.newInstance  has error:" + name, e);
            }
        }

        if (this.protocolType.equalsIgnoreCase(ConfigConstants.UDP_PROTOCOL)) {
            cp.addLast("execution", executionHandler);
        }

        return cp;
    }
}
