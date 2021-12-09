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

import com.google.common.base.Preconditions;
import java.lang.reflect.Constructor;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.FlumeException;
import org.apache.flume.conf.Configurable;
import org.apache.flume.conf.Configurables;
import org.apache.flume.source.AbstractSource;
import org.apache.inlong.dataproxy.consts.ConfigConstants;
import org.apache.inlong.dataproxy.utils.MonitorIndex;
import org.apache.inlong.dataproxy.utils.MonitorIndexExt;
import org.jboss.netty.bootstrap.Bootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * source base clase
 *
 */
public abstract class BaseSource
        extends AbstractSource
        implements EventDrivenSource, Configurable {
  private static final Logger logger = LoggerFactory.getLogger(BaseSource.class);

  protected Context context;

  protected int port;

  protected String host = null;

  protected String msgFactoryName;

  protected String serviceDecoderName;

  protected String messageHandlerName;

  protected int maxMsgLength;

  protected boolean isCompressed;

  protected String topic;

  protected String attr;

  protected boolean filterEmptyMsg;

  private int statIntervalSec;

  protected int pkgTimeoutSec;

  protected int maxConnections = Integer.MAX_VALUE;

  private static final String CONNECTIONS = "connections";

  protected boolean customProcessor = false;

  /*
   * monitor
   */
  private MonitorIndex monitorIndex;

  private MonitorIndexExt monitorIndexExt;

  /*
   * netty server
   */

  protected Bootstrap serverBootstrap = null;

  protected ChannelGroup allChannels;

  protected Channel nettyChannel = null;

  private static String HOST_DEFAULT_VALUE = "0.0.0.0";

  private static int maxMonitorCnt = 300000;

  private static int DEFAULT_MAX_CONNECTIONS = 5000;

  private static int STAT_INTERVAL_MUST_THAN = 0;

  private static int PKG_TIMEOUT_DEFAULT_SEC = 3;

  private static int MSG_MIN_LENGTH = 4;

  private static int MAX_MSG_DEFAULT_LENGTH = 1024 * 64;

  private static int INTERVAL_SEC = 60;

  protected static int DEFAULT_MAX_THREADS = 32;

  protected int maxThreads = 32;

  public BaseSource() {
    super();
    allChannels = new DefaultChannelGroup();
  }

  @Override
  public synchronized void start() {
    super.start();
    /*
     * init monitor logic
     */
    monitorIndex = new MonitorIndex("Source",INTERVAL_SEC, maxMonitorCnt);
    monitorIndexExt = new MonitorIndexExt("DataProxy_monitors#"
            + this.getProtocolName(),INTERVAL_SEC, maxMonitorCnt);
    startSource();
  }

  @Override
  public synchronized void stop() {
    logger.info("[STOP {} SOURCE]{} stopping...", this.getProtocolName(), this.getName());
    if (!allChannels.isEmpty()) {
      try {
        allChannels.close().awaitUninterruptibly();
      } catch (Exception e) {
        logger.warn("Simple UDP Source netty server stop ex, {}", e);
      } finally {
        allChannels.clear();
      }
    }

    if (serverBootstrap != null) {
      try {
        serverBootstrap.releaseExternalResources();
      } catch (Exception e) {
        logger.warn("Simple UDP Source serverBootstrap stop ex {}", e);
      } finally {
        serverBootstrap = null;
      }
    }
    super.stop();
    if (monitorIndex != null) {
      monitorIndex.shutDown();
    }
    if (monitorIndexExt != null) {
      monitorIndexExt.shutDown();
    }
    logger.info("[STOP {} SOURCE]{} stopped", this.getProtocolName(), this.getName());
  }

  @Override
  public void configure(Context context) {
    this.context = context;

    port = context.getInteger(ConfigConstants.CONFIG_PORT);

    host = context.getString(ConfigConstants.CONFIG_HOST, HOST_DEFAULT_VALUE);

    Configurables.ensureRequiredNonNull(context, ConfigConstants.CONFIG_PORT);

    Preconditions.checkArgument(ConfStringUtils.isValidIp(host), "ip config not valid");
    Preconditions.checkArgument(ConfStringUtils.isValidPort(port), "port config not valid");

    msgFactoryName =
            context.getString(ConfigConstants.MSG_FACTORY_NAME,
                    "org.apache.inlong.dataproxy.source.ServerMessageFactory");
    msgFactoryName = msgFactoryName.trim();
    Preconditions.checkArgument(StringUtils.isNotBlank(msgFactoryName),
            "msgFactoryName is empty");

    serviceDecoderName =
            context.getString(ConfigConstants.SERVICE_PROCESSOR_NAME,
                    "org.apache.inlong.dataproxy.source.DefaultServiceDecoder");
    serviceDecoderName = serviceDecoderName.trim();
    Preconditions.checkArgument(StringUtils.isNotBlank(serviceDecoderName),
            "serviceProcessorName is empty");

    messageHandlerName =
            context.getString(ConfigConstants.MESSAGE_HANDLER_NAME,
                    "org.apache.inlong.dataproxy.source.ServerMessageHandler");
    messageHandlerName = messageHandlerName.trim();
    Preconditions.checkArgument(StringUtils.isNotBlank(messageHandlerName),
            "messageHandlerName is empty");

    maxMsgLength = context.getInteger(ConfigConstants.MAX_MSG_LENGTH, MAX_MSG_DEFAULT_LENGTH);
    Preconditions.checkArgument(
            (maxMsgLength >= MSG_MIN_LENGTH && maxMsgLength <= ConfigConstants.MSG_MAX_LENGTH_BYTES),
            "maxMsgLength must be >= 4 and <= " + ConfigConstants.MSG_MAX_LENGTH_BYTES);
    isCompressed = context.getBoolean(ConfigConstants.MSG_COMPRESSED, true);

    filterEmptyMsg = context.getBoolean(ConfigConstants.FILTER_EMPTY_MSG, false);

    topic = context.getString(ConfigConstants.TOPIC);
    attr = context.getString(ConfigConstants.ATTR);
    Configurables.ensureRequiredNonNull(context, ConfigConstants.TOPIC, ConfigConstants.ATTR);

    topic = topic.trim();
    Preconditions.checkArgument(!topic.isEmpty(), "topic is empty");
    attr = attr.trim();
    Preconditions.checkArgument(!attr.isEmpty(), "attr is empty");

    filterEmptyMsg = context.getBoolean(ConfigConstants.FILTER_EMPTY_MSG, false);

    statIntervalSec = context.getInteger(ConfigConstants.STAT_INTERVAL_SEC, INTERVAL_SEC);
    Preconditions.checkArgument((statIntervalSec >= STAT_INTERVAL_MUST_THAN), "statIntervalSec must be >= 0");

    pkgTimeoutSec = context.getInteger(ConfigConstants.PACKAGE_TIMEOUT_SEC, PKG_TIMEOUT_DEFAULT_SEC);

    try {
      maxConnections = context.getInteger(CONNECTIONS, DEFAULT_MAX_CONNECTIONS);
    } catch (NumberFormatException e) {
      logger.warn("BaseSource\'s \"connections\" property must specify an integer value.",
          context.getString(CONNECTIONS));
    }

    try {
      maxThreads = context.getInteger(ConfigConstants.MAX_THREADS, DEFAULT_MAX_THREADS);
    } catch (NumberFormatException e) {
      logger.warn("Simple TCP Source max-threads property must specify an integer value. {}",
              context.getString(ConfigConstants.MAX_THREADS));
    }

    this.customProcessor = context.getBoolean(ConfigConstants.CUSTOM_CHANNEL_PROCESSOR, false);
  }

  /**
   * channel factory
   * @return
   */
  public ChannelPipelineFactory getChannelPiplineFactory() {
    logger.info(new StringBuffer("load msgFactory=").append(msgFactoryName)
            .append(" and serviceDecoderName=").append(serviceDecoderName).toString());
    ChannelPipelineFactory fac = null;
    try {
      ServiceDecoder serviceDecoder = (ServiceDecoder)Class.forName(serviceDecoderName).newInstance();
      Class<? extends ChannelPipelineFactory> clazz =
              (Class<? extends ChannelPipelineFactory>) Class.forName(msgFactoryName);
      Constructor ctor = clazz.getConstructor(AbstractSource.class, ChannelGroup.class,
              String.class, ServiceDecoder.class, String.class,
              String.class, String.class, Boolean.class,
              Integer.class, Boolean.class, MonitorIndex.class,
              MonitorIndexExt.class, String.class);
      logger.info("Using channel processor:{}", getChannelProcessor().getClass().getName());
      fac = (ChannelPipelineFactory) ctor.newInstance(this, allChannels,
              this.getProtocolName(), serviceDecoder, messageHandlerName, topic, attr, filterEmptyMsg,
              maxConnections, isCompressed,
              monitorIndex, monitorIndexExt, this.getProtocolName());
    } catch (Exception e) {
      logger.error(
              "Simple {} Source start error, fail to construct ChannelPipelineFactory with name "
                      + "{}, ex {}",this.getProtocolName(), msgFactoryName, e);
      stop();
      throw new FlumeException(e.getMessage());
    }
    return fac;
  }

  public Context getContext() {
    return context;
  }

  public abstract String getProtocolName();

  public abstract void startSource();

}
