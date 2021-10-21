/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.inlong.dataproxy.sink;

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.RateLimiter;
import org.apache.inlong.dataproxy.base.HighPriorityThreadFactory;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;

import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.apache.inlong.dataproxy.consts.AttributeConstants;
import org.apache.inlong.dataproxy.consts.ConfigConstants;
import org.apache.inlong.dataproxy.config.ConfigManager;
import org.apache.inlong.dataproxy.config.holder.ConfigUpdateCallback;
import org.apache.inlong.dataproxy.utils.FailoverChannelProcessorHolder;
import org.apache.inlong.dataproxy.utils.LogCounter;
import org.apache.inlong.dataproxy.utils.MonitorIndex;
import org.apache.inlong.dataproxy.utils.MonitorIndexExt;
import org.apache.inlong.dataproxy.utils.NetworkUtils;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.PulsarClientException.AlreadyClosedException;
import org.apache.pulsar.client.api.PulsarClientException.NotConnectedException;
import org.apache.pulsar.client.api.PulsarClientException.ProducerQueueIsFullError;
import org.apache.pulsar.client.api.PulsarClientException.TopicTerminatedException;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.jboss.netty.handler.codec.frame.TooLongFrameException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * use pulsarSink need adding such config, if these ara not config in flume.conf, PulsarSink will
 * use default value.
 * prefix of pulsar sink config in flume.conf like this XXX.sinks.XXX.property
 * and properties are may these Configurations:
 *  type (*): value must be 'org.apache.inlong.dataproxy.sink.PulsarSink'
 *  pulsar_server_url_list (*): value is pulsar broker url , like this 'pulsar://127.0.0.1:6650'
 *  send_timeout_MILL: send message timeout, unit is millisecond, default value is 30000 (mean 30s)
 *  stat_interval_sec: stat info will be made period time , unit is second, default value is 60s
 *  thread-num: sink thread num. default value  is 8
 *  client-id-cache: whether use cache in client, default value is true
 *  max_pending_messages: default value is 10000
 *  max_batching_messages: default value is 1000
 *  enable_batch: default is true
 *  block_if_queue_full: default is true
 */
public class PulsarSink extends AbstractSink implements Configurable {

    private static final Logger logger = LoggerFactory.getLogger(PulsarSink.class);

    /*
     * properties key for pulsar client
     */
    private static String PULSAR_SERVER_URL_LIST = "pulsar_server_url_list";
    private static String RETRY_CNT = "retry_currentSuccSendedCnt";
    private static String STAT_INTERVAL_SEC = "stat_interval_sec"; // in sec
    private static String CLIENT_ID_CACHE = "client_id_cache";

    /*
     * properties key pulsar producer
     */
    private static String SEND_TIMEOUT = "send_timeout_MILL";
    private static String ENABLE_BATCH = "enable_batch";
    private static String BLOCK_IF_QUEUE_FULL = "block_if_queue_full";
    private static String MAX_PENDING_MESSAGES = "max_pending_messages";
    private static String MAX_BATCHING_MESSAGES = "max_batching_messages";

    /*
     * properties for header info
     */
    private static String TOPIC = "topic";

    /*
     * default value
     */
    private static int BAD_EVENT_QUEUE_SIZE = 10000;
    private static int BATCH_SIZE = 10000;
    private static final int DEFAULT_RETRY_CNT = -1;
    private static final int DEFAULT_LOG_EVERY_N_EVENTS = 100000;
    private static final int DEFAULT_STAT_INTERVAL_SEC = 60;

    private static int DEFAULT_SEND_TIMEOUT_MILL = 30 * 1000;
    private static boolean DEFAULT_ENABLE_BATCH = true;
    private static boolean DEFAULT_BLOCK_IF_QUEUE_FULL = true;
    private static int DEFAULT_MAX_PENDING_MESSAGES = 10000;
    private static int DEFAULT_MAX_BATCHING_MESSAGES = 1000;

    /*
     * properties for stat
     */
    private static String LOG_EVERY_N_EVENTS = "log-every-n-events";

    /*
     * for pulsar client
     */
    private String pulsarServerUrlList;

    /*
     * for producer
     */
    private Integer sendTimeout; // in millsec
    private boolean enableBatch = true;
    private boolean blockIfQueueFull = true;
    private int maxPendingMessages = 10000;
    private int maxBatchingMessages = 1000;
    public Map<String, Producer> producerMap;
    public PulsarClient pulsarClient;

    /*
     * for log
     */
    private Integer logEveryNEvents;
    private long diskIORatePerSec;
    private RateLimiter diskRateLimiter;

    /*
     * for stat
     */
    private AtomicLong currentSuccessSendCnt = new AtomicLong(0);
    private AtomicLong lastSuccessSendCnt = new AtomicLong(0);
    private long t1 = System.currentTimeMillis();
    private long t2 = 0L;
    private static AtomicLong totalPulsarSuccSendCnt = new AtomicLong(0);
    private static AtomicLong totalPulsarSuccSendSize = new AtomicLong(0);
    /*
     * for control
     */
    private static int retryCnt = DEFAULT_RETRY_CNT;
    private boolean overflow = false;

    private LinkedBlockingQueue<EventStat> resendQueue;
    private SinkCounter sinkCounter;
    private int maxMonitorCnt = 300000;

    private int statIntervalSec = DEFAULT_STAT_INTERVAL_SEC;
    private long logCounter = 0;

    private final AtomicLong currentInFlightCount = new AtomicLong(0);

    private boolean clientIdCache = false;

    private static ConcurrentHashMap<String, Long> illegalTopicMap =
            new ConcurrentHashMap<String, Long>();

    /*
     * whether the SendTask thread can send data to pulsar
     */
    private volatile boolean canSend = false;

    /*
     * Control whether the SinkRunner thread can read data from the Channel
     */
    private volatile boolean canTake = false;

    /*
     * log tools
     */
    private static final LogCounter logPrinterA = new LogCounter(10, 100000, 60 * 1000);
    private static final LogCounter logPrinterB = new LogCounter(10, 100000, 60 * 1000);
    private static final LogCounter logPrinterC = new LogCounter(10, 100000, 60 * 1000);


    private static final String SINK_THREAD_NUM = "thread-num";
    private static int EVENT_QUEUE_SIZE = 1000;
    private int threadNum;


    /*
     * send thread pool
     */
    private Thread[] sinkThreadPool;
    private LinkedBlockingQueue<Event> eventQueue;

    private static final String SEPARATOR = "#";
    private boolean isNewMetricOn = true;
    private MonitorIndex monitorIndex;
    private MonitorIndexExt monitorIndexExt;
    private ConfigManager configManager;
    private Map<String, String> topicProperties;


    private static final Long PRINT_INTERVAL = 30L;
    private static final PulsarPerformanceTask pulsarPerformanceTask = new PulsarPerformanceTask();
    private static ScheduledExecutorService scheduledExecutorService = Executors
            .newScheduledThreadPool(1, new HighPriorityThreadFactory("pulsarPerformance-Printer-thread"));

    private static final  LoadingCache<String, Long> agentIdCache = CacheBuilder.newBuilder()
            .concurrencyLevel(4 * 8).initialCapacity(500).expireAfterAccess(30, TimeUnit.SECONDS)
            .build(new CacheLoader<String, Long>() {
                //默认的数据加载实现，当调用get取值时，如果key没有对应的值，则调用此方法进行加载。
                @Override
                public Long load(String key) {
                    return System.currentTimeMillis();
                }
            });

    static {
        /*
         * stat pulsar performance
         */
        System.out.println("pulsarPerformanceTask!!!!!!");
        scheduledExecutorService.scheduleWithFixedDelay(pulsarPerformanceTask, 0L,
                PRINT_INTERVAL, TimeUnit.SECONDS);
    }

    public PulsarSink() {
        super();
        logger.debug("new instance of PulsarSink!");
    }

    private PulsarClient initPulsarClient() throws Exception {
        pulsarClient = PulsarClient.builder()
                .serviceUrl(pulsarServerUrlList)
                .connectionTimeout(30, TimeUnit.SECONDS)
                .build();
        return null;
    }

    private void initTopicSet(Set<String> topicSet) throws Exception {
        long startTime = System.currentTimeMillis();
        if (topicSet != null) {
            for (String topic : topicSet) {
                getProducer(topic);
            }
        }
        logger.info(getName() + " initTopicSet cost: " + (System.currentTimeMillis() - startTime) + "ms");
        logger.info(getName() + " producer is ready for topics : " + producerMap.keySet());
    }

    public void configure(Context context) {
        logger.info("PulsarSink started and context = {}", context.toString());
        isNewMetricOn = context.getBoolean("new-metric-on", true);
        maxMonitorCnt = context.getInteger("max-monitor-cnt",300000);

        configManager = ConfigManager.getInstance();
        topicProperties = configManager.getTopicProperties();
        configManager.getTopicConfig().addUpdateCallback(new ConfigUpdateCallback() {
            @Override
            public void update() {
                diffSetPublish(new HashSet<String>(topicProperties.values()),
                        new HashSet<String>(configManager.getTopicProperties().values()));
            }
        });

        pulsarServerUrlList = context.getString(PULSAR_SERVER_URL_LIST);
        Preconditions.checkState(pulsarServerUrlList != null, "No master and port list specified");

        producerMap = new HashMap<String, Producer>();

        logEveryNEvents = context.getInteger(LOG_EVERY_N_EVENTS, DEFAULT_LOG_EVERY_N_EVENTS);
        logger.debug(this.getName() + " " + LOG_EVERY_N_EVENTS + " " + logEveryNEvents);
        Preconditions.checkArgument(logEveryNEvents > 0, "logEveryNEvents must be > 0");

        sendTimeout = context.getInteger(SEND_TIMEOUT, DEFAULT_SEND_TIMEOUT_MILL);
        logger.debug(this.getName() + " " + SEND_TIMEOUT + " " + sendTimeout);
        Preconditions.checkArgument(sendTimeout > 0, "sendTimeout must be > 0");

        enableBatch = context.getBoolean(ENABLE_BATCH, DEFAULT_ENABLE_BATCH);
        blockIfQueueFull = context.getBoolean(BLOCK_IF_QUEUE_FULL, DEFAULT_BLOCK_IF_QUEUE_FULL);
        maxPendingMessages = context.getInteger(MAX_PENDING_MESSAGES, DEFAULT_MAX_PENDING_MESSAGES);
        maxBatchingMessages =  context.getInteger(MAX_BATCHING_MESSAGES, DEFAULT_MAX_BATCHING_MESSAGES);

        retryCnt = context.getInteger(RETRY_CNT, DEFAULT_RETRY_CNT);
        logger.debug(this.getName() + " " + RETRY_CNT + " " + retryCnt);

        statIntervalSec = context.getInteger(STAT_INTERVAL_SEC, DEFAULT_STAT_INTERVAL_SEC);
        logger.debug(this.getName() + " " + STAT_INTERVAL_SEC + " " + statIntervalSec);
        Preconditions.checkArgument(statIntervalSec >= 0, "statIntervalSec must be >= 0");

        clientIdCache = context.getBoolean(CLIENT_ID_CACHE, clientIdCache);

        if (sinkCounter == null) {
            sinkCounter = new SinkCounter(getName());
        }

        resendQueue = new LinkedBlockingQueue<EventStat>(BAD_EVENT_QUEUE_SIZE);

        String sinkThreadNum = context.getString(SINK_THREAD_NUM, "4");
        threadNum = Integer.parseInt(sinkThreadNum);
        Preconditions.checkArgument(threadNum > 0, "threadNum must be > 0");
        sinkThreadPool = new Thread[threadNum];
        eventQueue = new LinkedBlockingQueue<Event>(EVENT_QUEUE_SIZE);

        diskIORatePerSec = context.getLong("disk-io-rate-per-sec",0L);
        if (diskIORatePerSec != 0) {
            diskRateLimiter = RateLimiter.create(diskIORatePerSec);
        }
    }

    /**
     * When topic.properties is re-enabled, the producer update is triggered
     * @param originalSet
     * @param endSet
     */
    public void diffSetPublish(Set<String> originalSet, Set<String> endSet) {

        boolean changed = false;
        for (String s : endSet) {
            if (!originalSet.contains(s)) {
                changed = true;
                try {
                    getProducer(s);
                } catch (Exception e) {
                    logger.error("Get producer failed!", e);
                }
            }
        }

        if (changed) {
            logger.info("topics.properties has changed, trigger diff publish for {}", getName());
            topicProperties = configManager.getTopicProperties();
        }
    }

    /**
     * If this function is called successively without calling {@see #destroyConnection()}, only the
     * first call has any effect.
     *
     * @throws FlumeException if an RPC client connection could not be opened
     */
    private void createConnection() throws FlumeException {
        if (pulsarClient != null) {
            return;
        }
        try {
            initPulsarClient();
            sinkCounter.incrementConnectionCreatedCount();
        } catch (PulsarClientException e) {
            sinkCounter.incrementConnectionFailedCount();
            logger.error("create connnection error in metasink, "
                        + "maybe pulsar master set error, please re-check. ex1 {}", e.getMessage());
            throw new FlumeException("connect to pulsar error1, "
                        + "maybe zkstr/zkroot set error, please re-check");
        } catch (Throwable e) {
            sinkCounter.incrementConnectionFailedCount();
            logger.error("create connnection error in metasink, "
                                + "maybe pulsar master set error/shutdown in progress, please re-check. ex2 {}",
                        e.getMessage());
            throw new FlumeException("connect to meta error2, "
                        + "maybe pulsar master set error/shutdown in progress, please re-check");
        }

        if (producerMap == null) {
            producerMap = new HashMap<String, Producer>();
        }
        logger.debug("building pulsar producer");
    }

    private Producer getProducer(String topic) throws Exception {
        if (producerMap.containsKey(topic)) {
            Producer producer = producerMap.get(topic);
            if (producer.isConnected()) {
                return producer;
            }
        }
        synchronized (this) {
            Producer producer = producerMap.get(topic);
            if (producer == null || !producer.isConnected()) {
                if (producer != null) {
                    producer.closeAsync();
                    logger.warn("[{}] producer is not connected, producer will be recreate", topic);
                }
                producer = pulsarClient.newProducer().sendTimeout(sendTimeout,
                        TimeUnit.MILLISECONDS)
                        .topic(topic)
                        .enableBatching(enableBatch)
                        .blockIfQueueFull(blockIfQueueFull)
                        .maxPendingMessages(maxPendingMessages)
                        .batchingMaxMessages(maxBatchingMessages)
                        .create();
                producerMap.put(topic, producer);
            }
        }
        return producerMap.get(topic);
    }

    private void destroyConnection() {
        producerMap.clear();
        if (pulsarClient != null) {
            try {
                pulsarClient.shutdown();
            } catch (PulsarClientException e) {
                logger.error("destroy pulsarClient error in PulsarSink, PulsarClientException {}",
                        e.getMessage());
            } catch (Exception e) {
                logger.error("destroy pulsarClient error in PulsarSink, ex {}", e.getMessage());
            }
        }
        pulsarClient = null;
        logger.debug("closed meta producer");
    }

    @Override
    public void start() {
        logger.info("pulsar sink starting...");
        sinkCounter.start();

        try {
            createConnection();
        } catch (FlumeException e) {
            logger.error("Unable to create pulsar client" + ". Exception follows.", e);
            /*
             * Try to prevent leaking resources.
             */
            destroyConnection();
            /*
             * FIXME: Mark ourselves as failed.
             */
            stop();
            return;
        }

        if (statIntervalSec > 0) {
            /*
             * switch for lots of metrics
             */
            if (isNewMetricOn) {
                monitorIndex = new MonitorIndex("Sink", statIntervalSec, maxMonitorCnt);
            }

            monitorIndexExt = new MonitorIndexExt("TDBus_monitors#" + this.getName(),
                    statIntervalSec, maxMonitorCnt);
        }

        super.start();
        this.canSend = true;
        this.canTake = true;

        try {
            initTopicSet(new HashSet<String>(topicProperties.values()));
        } catch (Exception e) {
            logger.info("meta sink start publish topic fail.",e);
        }

        for (int i = 0; i < sinkThreadPool.length; i++) {
            sinkThreadPool[i] = new Thread(new SinkTask(), getName()
                    + "_pulsar_sink_sender-"
                    + i);
            sinkThreadPool[i].start();
        }

        logger.debug("meta sink started");
    }

    @Override
    public void stop() {
        logger.info("pulsar sink stopping");
        this.canTake = false;
        int waitCount = 0;
        while (eventQueue.size() != 0 && waitCount++ < 10) {
            try {
                Thread.currentThread().sleep(1000);
            } catch (InterruptedException e) {
                logger.info("Stop thread has been interrupt!");
                break;
            }
        }
        this.canSend = false;

        destroyConnection();
        sinkCounter.stop();

        if (statIntervalSec > 0) {
            try {
                monitorIndex.shutDown();
            } catch (Exception e) {
                logger.warn("statrunner interrupted");
            }
        }

        if (sinkThreadPool != null) {
            for (Thread thread : sinkThreadPool) {
                if (thread != null) {
                    thread.interrupt();
                }
            }
            sinkThreadPool = null;
        }

        super.stop();
        if (!scheduledExecutorService.isShutdown()) {
            scheduledExecutorService.shutdown();
        }
        logger.debug("pulsar sink stopped. Metrics:{}", sinkCounter);
    }

    @Override
    public Status process() throws EventDeliveryException {
        if (!this.canTake) {
            return Status.BACKOFF;
        }

        Status status = Status.READY;
        Channel channel = getChannel();
        Transaction tx = channel.getTransaction();
        tx.begin();
        try {
            Event event = channel.take();
            if (event != null) {
                if (diskRateLimiter != null) {
                    diskRateLimiter.acquire(event.getBody().length);
                }
                if (!eventQueue.offer(event, 3 * 1000, TimeUnit.MILLISECONDS)) {
                    logger.info("[{}] Channel --> Queue(has no enough space,current code point) "
                            + "--> pulsar,Check if pulsar server or network is ok.(if this situation "
                            + "last long time it will cause memoryChannel full and fileChannel write.)", getName());
                    tx.rollback();
                } else {
                    tx.commit();
                }
            } else {
                status = Status.BACKOFF;
                tx.commit();
            }
        } catch (Throwable t) {
            logger.error("Process event failed!" + this.getName(), t);
            try {
                tx.rollback();
            } catch (Throwable e) {
                logger.error("metasink transaction rollback exception",e);

            }
        } finally {
            tx.close();
        }
        return status;
    }

    private void editStatistic(final Event event, String keyPostfix, String msgId) {
        String topic = "";
        String tid = "";
        String nodeIp = null;
        if (event != null) {
            if (event.getHeaders().containsKey(TOPIC)) {
                topic = event.getHeaders().get(TOPIC);
            }
            if (event.getHeaders().containsKey(AttributeConstants.INTERFACE_ID)) {
                tid = event.getHeaders().get(AttributeConstants.INTERFACE_ID);
            } else if (event.getHeaders().containsKey(AttributeConstants.INAME)) {
                tid = event.getHeaders().get(AttributeConstants.INAME);
            }

            /*
             * Compatible agent
             */
            if (event.getHeaders().containsKey("ip")) {
                event.getHeaders().put(ConfigConstants.REMOTE_IP_KEY, event.getHeaders().get("ip"));
                event.getHeaders().remove("ip");
            }

            /*
             * Compatible agent
             */
            if (event.getHeaders().containsKey("time")) {
                event.getHeaders().put(AttributeConstants.DATA_TIME,event.getHeaders().get("time"));
                event.getHeaders().remove("time");
            }

            if (event.getHeaders().containsKey(ConfigConstants.REMOTE_IP_KEY)) {
                nodeIp = event.getHeaders().get(ConfigConstants.REMOTE_IP_KEY);
                if (event.getHeaders().containsKey(ConfigConstants.REMOTE_IDC_KEY)) {

                    if (nodeIp != null) {
                        nodeIp = nodeIp.split(":")[0];
                    }

                    long tMsgCounterL = 1L;
                    /*
                     * msg counter
                     */
                    if (event.getHeaders().containsKey(ConfigConstants.MSG_COUNTER_KEY)) {
                        tMsgCounterL = Integer.parseInt(event.getHeaders()
                                .get(ConfigConstants.MSG_COUNTER_KEY));
                    }

                    /*
                     * SINK_INTF#metasink1#topic#tid#clientIp#busIP#pkgTime#successCnt#packcnt
                     * #packsize#failCnt
                     */
                    StringBuilder newbase = new StringBuilder();
                    newbase.append(this.getName()).append(SEPARATOR).append(topic).append(SEPARATOR)
                            .append(tid).append(SEPARATOR).append(nodeIp)
                            .append(SEPARATOR).append(NetworkUtils.getLocalIp())
                            .append(SEPARATOR).append(msgId).append(SEPARATOR)
                            .append(event.getHeaders().get(ConfigConstants.PKG_TIME_KEY));

                    long messageSize = event.getBody().length;

                    if (event.getHeaders().get(ConfigConstants.TOTAL_LEN) != null) {
                        messageSize = Long.parseLong(event.getHeaders().get(ConfigConstants.TOTAL_LEN));
                    }

                    if (keyPostfix != null && !keyPostfix.equals("")) {
                        monitorIndex.addAndGet(new String(newbase), 0, 0, 0, (int) tMsgCounterL);
                        if (logPrinterB.shouldPrint()) {
                            logger.warn("error cannot send event, {} event size is {}", topic, messageSize);
                        }
                    } else {
                        monitorIndex.addAndGet(new String(newbase), (int) tMsgCounterL, 1, messageSize, 0);
                    }
                }
            }
        }
    }

    public void handleMessageSendSuccess(final MessageIdImpl result, EventStat eventStat) {
        /*
         * Statistics pulsar performance
         */
        totalPulsarSuccSendCnt.incrementAndGet();
        totalPulsarSuccSendSize.addAndGet(eventStat.getEvent().getBody().length);
        /*
         *add to sinkCounter
         */
        sinkCounter.incrementEventDrainSuccessCount();
        currentInFlightCount.decrementAndGet();
        currentSuccessSendCnt.incrementAndGet();
        long nowCnt = currentSuccessSendCnt.get();
        long oldCnt = lastSuccessSendCnt.get();
        if (nowCnt % logEveryNEvents == 0 && nowCnt != lastSuccessSendCnt.get()) {
            lastSuccessSendCnt.set(nowCnt);
            t2 = System.currentTimeMillis();
            logger.info("metasink {}, succ put {} events to pulsar,"
                    + " in the past {} millsec", new Object[] {
                    getName(), (nowCnt - oldCnt), (t2 - t1)
            });
            t1 = t2;
        }
        monitorIndexExt.incrementAndGet("METASINK_SUCCESS");
        editStatistic(eventStat.event, null, result.toString());
    }

    public void handleMessageSendException(final Object e, EventStat eventStat) {
        monitorIndexExt.incrementAndGet("METASINK_EXP");
        if (e instanceof TooLongFrameException) {
            PulsarSink.this.overflow = true;
        } else if (e instanceof ProducerQueueIsFullError) {
            PulsarSink.this.overflow = true;
        } else if (!(e instanceof AlreadyClosedException
                || e instanceof NotConnectedException
                || e instanceof TopicTerminatedException)) {
            if (logPrinterB.shouldPrint()) {
                logger.error("Send failed!{}{}", getName(), e);
            }
            if (eventStat.getRetryCnt() == 0) {
                editStatistic(eventStat.event, "failure", "");
            }
        }
        eventStat.incRetryCnt();
        resendEvent(eventStat, true);
    }

    /**
     * Resend the data and store the data in the memory cache.
     * @param es
     * @param isDecrement
     */
    private void resendEvent(EventStat es, boolean isDecrement) {
        try {
            if (isDecrement) {
                currentInFlightCount.decrementAndGet();
            }
            if (es == null || es.getEvent() == null) {
                return;
            }
            /*
             * If the failure requires retransmission to pulsar,
             * the sid needs to be removed before retransmission.
             */
            if (clientIdCache) {
                String clientId = es.getEvent().getHeaders().get(ConfigConstants.SEQUENCE_ID);
                if (clientId != null && agentIdCache.asMap().containsKey(clientId)) {
                    agentIdCache.invalidate(clientId);
                }
            }
            if (!resendQueue.offer(es)) {
                /*
                 * If the retry queue is full, it will re-enter the channel; if it is not full,
                 *  it will be taken out of the resendQueue in sinktask
                 */
                FailoverChannelProcessorHolder.getChannelProcessor().processEvent(es.getEvent());
                if (logPrinterC.shouldPrint()) {
                    logger.error(getName() + " Channel --> pulsar --> ResendQueue(full) "
                            + "-->FailOverChannelProcessor(current code point), "
                            + "Resend queue is full,Check if pulsar server or network is ok.");
                }
            }
        } catch (Throwable throwable) {
            monitorIndexExt.incrementAndGet("PULSAR_SINK_DROPPED");
            if (logPrinterC.shouldPrint()) {
                logger.error(getName() + " Discard msg because put events to both of "
                        + "queue and fileChannel fail,current resendQueue.size = "
                        + resendQueue.size(), throwable);
            }
        }
    }

    static class PulsarPerformanceTask implements Runnable {
        @Override
        public void run() {
            try {
                if (totalPulsarSuccSendSize.get() != 0) {
                    logger.info("Total pulsar performance tps :"
                            + totalPulsarSuccSendCnt.get() / PRINT_INTERVAL
                            + "/s, avg msg size:"
                            + totalPulsarSuccSendSize.get() / totalPulsarSuccSendCnt.get()
                            + ",print every " + PRINT_INTERVAL + " seconds");
                    /*
                     * totalpulsarSuccSendCnt represents the number of packets
                     */
                    totalPulsarSuccSendCnt.set(0);
                    totalPulsarSuccSendSize.set(0);
                }

            } catch (Exception e) {
                logger.info("pulsarPerformanceTask error", e);
            }
        }
    }

    public static class EventStat {
        private Event event;
        private int myRetryCnt;

        public EventStat(Event event) {
            this.event = event;
            this.myRetryCnt = 0;
        }

        public EventStat(Event event, int retryCnt) {
            this.event = event;
            this.myRetryCnt = retryCnt;
        }

        public Event getEvent() {
            return event;
        }

        public void setEvent(Event event) {
            this.event = event;
        }

        public int getRetryCnt() {
            return myRetryCnt;
        }

        public void setRetryCnt(int retryCnt) {
            this.myRetryCnt = retryCnt;
        }

        public void incRetryCnt() {
            this.myRetryCnt++;
        }

        public boolean shouldDrop() {
            /*
             * lost messages only happen when resendqueue is full.
             * Theoretically resendqueue may be not full,
             * Because the data fetched from the channel has a limit on
             * the number of unresponsive data sent.
             *  The number of entries is smaller than the queue size
             */
            return false;
        }

        public void reset() {
            this.event = null;
            this.myRetryCnt = 0;
        }
    }

    class SinkTask implements Runnable {
        @Override
        public void run() {
            logger.info("Sink task {} started.", Thread.currentThread().getName());
            while (canSend) {
                boolean decrementFlag = false;
                boolean resendBadEvent = false;
                Event event = null;
                EventStat eventStat = null;
                String topic = null;
                try {
                    if (PulsarSink.this.overflow) {
                        PulsarSink.this.overflow = false;
                        Thread.currentThread().sleep(10);
                    }
                    if (!resendQueue.isEmpty()) {
                        /*
                         * Send the data in the retry queue first
                         */
                        eventStat = resendQueue.poll();
                        if (eventStat != null) {
                            event = eventStat.getEvent();
                            // logger.warn("Resend event: {}", event.toString());
                            if (event.getHeaders().containsKey(TOPIC)) {
                                topic = event.getHeaders().get(TOPIC);
                            }
                            resendBadEvent = true;
                        }
                    } else {
                        if (currentInFlightCount.get() > BATCH_SIZE) {
                            /*
                             * Under the condition that the number of unresponsive messages
                             * is greater than 1w, the number of unresponsive messages sent
                             * to pulsar will be printed periodically
                             */
                            logCounter++;
                            if (logCounter == 1 || logCounter % 100000 == 0) {
                                logger.info(getName()
                                                + " currentInFlightCount={} resendQueue"
                                                + ".size={}",
                                        currentInFlightCount.get(),resendQueue.size());
                            }
                            if (logCounter > Long.MAX_VALUE - 10) {
                                logCounter = 0;
                            }
                        }
                        event = eventQueue.take();
                        eventStat = new EventStat(event);
                        sinkCounter.incrementEventDrainAttemptCount();
                        if (event.getHeaders().containsKey(TOPIC)) {
                            topic = event.getHeaders().get(TOPIC);
                        }
                    }
                    logger.debug("Event is {}, topic = {} ",event, topic);

                    if (event == null) {
                        continue;
                    }

                    if (topic == null || topic.equals("")) {
                        logger.warn("no topic specified in event header, just skip this event");
                        continue;
                    }

                    Long expireTime = illegalTopicMap.get(topic);
                    if (expireTime != null) {
                        long currentTime = System.currentTimeMillis();
                        if (expireTime > currentTime) {
                            /*
                             * If the exception-channel is configured, put the exception-channel,
                             * otherwise discard the data
                             */
                            continue;
                        } else {
                            /*
                             * Illegal has expired, there is no need to put it in the map
                             */
                            illegalTopicMap.remove(topic);
                        }
                    }
                    Producer producer = null;
                    try {
                        producer = getProducer(topic);
                    } catch (Exception e) {
                        if (logPrinterA.shouldPrint()) {
                            /*
                             * If it is not an IllegalTopicException,
                             * the producer may be null,
                             * causing the sendMessage part to report a null pointer later
                             */
                            logger.error("Get producer failed!", e);
                        }
                    }
                    /*
                     * If the producer is a null value,\ it means that the topic is not yet
                     * ready, and it needs to be played back into the file channel
                     */
                    if (producer == null) {
                        /*
                         * Data within 30s is placed in the exception channel to
                         * prevent frequent checks
                         * After 30s, reopen the topic check, if it is still a null value,
                         *  put it back into the illegal map
                         */
                        illegalTopicMap.put(topic, System.currentTimeMillis() + 30 * 1000);
                        continue;
                    }

                    String clientId = event.getHeaders().get(ConfigConstants.SEQUENCE_ID);
                    final EventStat es = eventStat;

                    boolean hasKey = false;
                    if (clientIdCache && clientId != null) {
                        hasKey = agentIdCache.asMap().containsKey(clientId);
                    }

                    if (clientIdCache && clientId != null && hasKey) {
                        agentIdCache.put(clientId, System.currentTimeMillis());
                        if (logPrinterA.shouldPrint()) {
                            logger.info("{} agent package {} existed,just discard.",
                                    getName(), clientId);
                        }
                    } else {
                        if (clientId != null) {
                            agentIdCache.put(clientId, System.currentTimeMillis());
                        }
                        Map<String, String> proMap = new HashMap<>();
                        proMap.put("tdbusip", NetworkUtils.getLocalIp());
                        String tid = "";
                        if (event.getHeaders().containsKey(AttributeConstants.INTERFACE_ID)) {
                            tid = event.getHeaders().get(AttributeConstants.INTERFACE_ID);
                        } else if (event.getHeaders().containsKey(AttributeConstants.INAME)) {
                            tid = event.getHeaders().get(AttributeConstants.INAME);
                        }
                        proMap.put(tid, event.getHeaders().get(ConfigConstants.PKG_TIME_KEY));
                        logger.debug("producer send msg!");
                        producer.newMessage().properties(proMap).value(event.getBody())
                                .sendAsync().thenAccept((msgId) -> {
                            handleMessageSendSuccess((MessageIdImpl)msgId, es);
                        }).exceptionally((e) -> {
                            handleMessageSendException(e, es);
                            return null;
                        });
                        currentInFlightCount.incrementAndGet();
                        decrementFlag = true;
                    }
                    /*
                     * No exception is thrown, after a complete one-time sending,
                     * the topic can be deleted from the illegal list
                     */
                    illegalTopicMap.remove(topic);
                } catch (InterruptedException e) {
                    logger.info("Thread {} has been interrupted!", Thread.currentThread().getName());
                    return;
                } catch (Throwable t) {
                    if (t instanceof PulsarClientException) {
                        String message = t.getMessage();
                        if (message != null && (message.contains("No available queue for topic")
                                || message.contains("The brokers of topic are all forbidden"))) {
                            illegalTopicMap.put(topic, System.currentTimeMillis() + 60 * 1000);
                            logger.info("IllegalTopicMap.put " + topic);
                            continue;
                        } else {
                            try {
                                /*
                                 * The exception of pulsar will cause the sending thread to block
                                 * and prevent further pressure on pulsar. Here you should pay
                                 * attention to the type of exception to prevent the error of
                                 *  a topic from affecting the global
                                 */
                                Thread.sleep(100);
                            } catch (InterruptedException e) {
                                //ignore..
                            }
                        }
                    }
                    if (logPrinterA.shouldPrint()) {
                        logger.error("Sink task fail to send the message, decrementFlag="
                                + decrementFlag
                                + ",sink.name="
                                + Thread.currentThread().getName()
                                + ",event.headers="
                                + eventStat.getEvent().getHeaders(), t);
                    }
                    /*
                     * producer.sendMessage is abnormal,
                     * so currentInFlightCount is not added,
                     * so there is no need to subtract
                     */
                    resendEvent(eventStat, decrementFlag);
                }
            }
        }
    }

}
