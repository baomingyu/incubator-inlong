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

package org.apache.inlong.reco.service.impl;

import org.apache.inlong.reco.dao.reco.ReconHumanNotifyDao;
import org.apache.inlong.reco.dao.reco.ReconStatusDao;
import org.apache.inlong.reco.dto.RecoStatusPo;
import org.apache.inlong.reco.dto.StatusNotifyPo;
import org.apache.inlong.reco.pojo.RecoTriggerMsg;
import org.apache.inlong.reco.pulsar.PulsarProducerService;
import org.apache.inlong.reco.service.AuditCheckerService;
import org.apache.inlong.reco.service.RecoStatus;
import org.apache.inlong.reco.service.ReconciliationCheckService;
import org.apache.inlong.reco.service.dispatch.CheckThread;
import org.apache.inlong.reco.service.dispatch.DispatchThread;
import org.apache.inlong.reco.service.dispatch.RecoveryThread;
import org.apache.inlong.reco.service.factory.DefaultThreadFactory;
import org.apache.inlong.reco.service.wheel.TimeWheel;
import org.apache.inlong.reco.util.DateUtils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.pagehelper.PageHelper;
import com.github.pagehelper.PageInfo;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

@Service
public class ReconciliationCheckServiceImpl
        implements
            InitializingBean,
            ReconciliationCheckService,
            Closeable {

    private static Logger LOGGER = LoggerFactory.getLogger(ReconciliationCheckServiceImpl.class);

    private static Logger RECO_TRIGGER_WATER_LOGGER = LoggerFactory.getLogger("RecoTriggerWaterLog");

    private static Logger RECO_SYS_MONITOR_LOGGER = LoggerFactory.getLogger("RecoSysMonitorLog");

    private static Logger API_NOTIFY_MSG_LOGGER = LoggerFactory.getLogger("ApiNotifyMsgLog");

    private static String TIMER_WHEEL_RETRY_TIME_AND_DELAY_TIME = "5,6,7,8,9,10,11,12,12,12";

    private static int TIMER_WHEEL_DEFAULT_RANGE = 5;

    private final ObjectMapper jasonMapper = new ObjectMapper();

    @Autowired
    private ReconStatusDao reconStatusDao;

    @Autowired
    private ReconHumanNotifyDao reconHumanNotifyDao;

    @Value("${inlong.audit.reconciliation.wheel.retry.config}")
    private String timerWheelRetryAndTimeConfig;

    @Value("${inlong.audit.reconciliation.wheel.max.delay.time:12}")
    private Integer timerWheelMaxDelayTime;

    @Value("${inlong.audit.reconciliation.max.msg.count.pre.queue:20000}")
    private Integer maxMsgCountPreQueue;

    @Value("${inlong.audit.reconciliation.dispatch.queue:5000}")
    private Integer dispatchQueueSize;

    @Value("${inlong.audit.reconciliation.check.queue:1000}")
    private Integer checkQueueSize;

    @Value("${inlong.audit.reconciliation.check.thread.num}")
    private int checkThreadNum;

    @Value("${inlong.audit.reconciliation.notify.pulsar.broker}")
    private String recoNotifyPulsarBroker;

    @Value("${inlong.audit.reconciliation.notify.pulsar.topic}")
    private String recoNotifyTopic;

    @Value("${inlong.audit.reconciliation.check.ratio}")
    private double recoCheckRatio = 0.9995;

    @Value("${inlong.audit.reconciliation.handle.tag}")
    private String handleTag;

    @Value("${inlong.audit.reconciliation.back.recovery.days:2}")
    private int backRecoveryDays;

    private CheckThread[] checkThreadArray;

    @Autowired
    private Map<String, AuditCheckerService> checkerMap;

    private ArrayBlockingQueue<RecoStatusPo> dispatchQueue;

    private Integer maxDelayTime = 12;

    private Integer rangeSize = null;

    private AtomicLong totalMsgNum = new AtomicLong(0L);

    private AtomicLong totalCheckMsgNum = new AtomicLong(0L);

    private DispatchThread dispatchThread;

    private TimeWheel timeWheel;

    private PulsarProducerService notifyUsPulsarProducerService;

    private int[] timerWheelDelayTimeRange;

    private AtomicLong handlerIndex = new AtomicLong(0);

    private AtomicBoolean isStopped = new AtomicBoolean(false);

    private ScheduledExecutorService sysMetricScheduledPool = Executors.newScheduledThreadPool(1,
            new DefaultThreadFactory("Reco-system-metric-thread"));

    private ScheduledExecutorService recoveryScheduledPool = Executors.newScheduledThreadPool(1,
            new DefaultThreadFactory("Reco-recovery-check-thread"));

    @Override
    public Boolean handleRecoMsgData(RecoStatusPo recoStatusPo) throws Exception {
        int retryTime = 0;
        while (true && !isStopped.get()) {
            int checkThreadIndex = (int) (handlerIndex.incrementAndGet() % checkThreadNum);
            if (checkThreadArray[checkThreadIndex].offerMsg(recoStatusPo)) {
                updateCheckTotalMsgNum(1);
                break;
            } else if (retryTime > checkThreadNum) {
                LOGGER.warn("handleRecoMsgData offerMsg exceed max retryTime {}", recoStatusPo);
                return false;
            }
            retryTime++;
        }
        return true;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        if (checkThreadNum <= 0) {
            checkThreadNum = Runtime.getRuntime().availableProcessors() * 2;
        }

        notifyUsPulsarProducerService = new PulsarProducerService(recoNotifyPulsarBroker, recoNotifyTopic);

        sysMetricScheduledPool.scheduleWithFixedDelay(() -> printMetric(), 1, 1, TimeUnit.MINUTES);

        recoveryScheduledPool.scheduleWithFixedDelay(new RecoveryThread(this, 1),
                2, 2, TimeUnit.HOURS);

        dispatchQueue = new ArrayBlockingQueue<>(dispatchQueueSize);

        if (StringUtils.isEmpty(timerWheelRetryAndTimeConfig)) {
            timerWheelRetryAndTimeConfig = TIMER_WHEEL_RETRY_TIME_AND_DELAY_TIME;
        }
        String[] timerWheelDelayRange = timerWheelRetryAndTimeConfig.split(",");

        if (timerWheelDelayRange != null && timerWheelDelayRange.length > 0) {
            rangeSize = timerWheelDelayRange.length;
            timerWheelDelayTimeRange = new int[timerWheelDelayRange.length];
            int i = 0;
            for (String delayTime : timerWheelDelayRange) {
                if (StringUtils.isNotEmpty(delayTime) && StringUtils.isNumeric(delayTime)) {
                    timerWheelDelayTimeRange[i] = Integer.parseInt(delayTime);
                    if (timerWheelDelayTimeRange[i] > maxDelayTime
                            && timerWheelDelayTimeRange[i] <= timerWheelMaxDelayTime) {
                        maxDelayTime = timerWheelDelayTimeRange[i];
                    } else if (timerWheelDelayTimeRange[i] > timerWheelMaxDelayTime) {
                        timerWheelDelayTimeRange[i] = timerWheelMaxDelayTime;
                        maxDelayTime = timerWheelMaxDelayTime;
                    }
                } else {
                    timerWheelDelayTimeRange[i] = TIMER_WHEEL_DEFAULT_RANGE;
                }
                i++;
            }
        }

        /*
         * per minutes handle once queue msg.
         */
        timeWheel = new TimeWheel(maxDelayTime, 1, maxMsgCountPreQueue, this);

        checkThreadArray = new CheckThread[checkThreadNum];
        for (int i = 0; i < checkThreadNum; i++) {
            checkThreadArray[i] = new CheckThread(this,
                    checkQueueSize, recoNotifyPulsarBroker, recoNotifyTopic);
            checkThreadArray[i].setName("reco-check-" + i);
            checkThreadArray[i].start();
        }

        dispatchThread = new DispatchThread(dispatchQueue, this);
        dispatchThread.setName("Reco-dispatcher");
        dispatchThread.start();

        new RecoveryThread(this, backRecoveryDays).start();
    }

    @Override
    public Boolean offerMsg(RecoTriggerMsg recoTriggerMsg, String msgId) throws Exception {
        initRecoTriggerMsg(recoTriggerMsg);
        printTriggerRecoWaterLog(recoTriggerMsg, msgId);
        RecoStatusPo po = convertMsgToPo(recoTriggerMsg, msgId);
        this.updateTotalMsgNum(1);
        if (handleTriggerRecoData(po)) {
            while (!dispatchQueue.offer(po, 30, TimeUnit.SECONDS)) {
                LOGGER.warn("Waiting for offer trigger msg to queue, and dispatch queue size = ", dispatchQueue.size());
            }
            return true;
        }
        return false;
    }

    public Boolean handleTriggerRecoData(RecoStatusPo po) {
        if (po != null && reconStatusDao != null) {
            try {
                reconStatusDao.insert(po);
            } catch (DuplicateKeyException e) {
                po.setRetryTime(0);
                handleUpdateCheckRecoData(po);
            } catch (Throwable e) {
                LOGGER.warn("handleInsertRecoData has exception ", e);
                return false;
            }
        }
        return true;
    }

    public RecoStatus handleRecoCheck(RecoStatusPo recoStatusPo) {
        try {
            AuditCheckerService checkerService = checkerMap.get(recoStatusPo.getAuditType());
            if (checkerService != null) {
                if (checkerService.check(recoStatusPo)) {
                    return RecoStatus.SUCCESS;
                } else {
                    return RecoStatus.RETRY;
                }
            } else {
                LOGGER.warn("AuditCheck is not found, TopoName = {}, groupId = {}, streamId = {}, auditType = {}",
                        recoStatusPo.getTopoName(), recoStatusPo.getInLongGroupId(), recoStatusPo.getInLongStreamId(),
                        recoStatusPo.getAuditType());
                return RecoStatus.ABANDON;
            }
        } catch (Throwable e) {
            LOGGER.error("RecoStatusPo [{}] check has exception ", recoStatusPo, e);
            return RecoStatus.RETRY;
        }
    }

    public String sendRecoNotifyMsg(StatusNotifyPo statusNotifyPo) {
        try {
            String msgId = notifyUsPulsarProducerService.sendNotifyMsg(jasonMapper
                    .writeValueAsString(statusNotifyPo).getBytes(StandardCharsets.UTF_8));
            statusNotifyPo.setSendMsgId(msgId);
            try {
                reconHumanNotifyDao.insert(statusNotifyPo);
            } catch (Exception e) {
                LOGGER.error("RecoNotifyMsg insert water log to db has error!", e);
            }
            API_NOTIFY_MSG_LOGGER.info(statusNotifyPo.getTopoName()
                    + "|" + statusNotifyPo.getPartition()
                    + "|" + statusNotifyPo.getInLongGroupId()
                    + "|" + statusNotifyPo.getInLongStreamId()
                    + "|" + statusNotifyPo.getDatabase()
                    + "|" + statusNotifyPo.getTable()
                    + "|" + statusNotifyPo.getUsTaskId()
                    + "|" + statusNotifyPo.getTimestamp()
                    + "|" + msgId);
            return msgId;
        } catch (JsonProcessingException e) {
            LOGGER.error("sendRecoNotifyMsg has error!", e);
        }
        return null;
    }

    public String sendTriggerRecoMsg(RecoTriggerMsg recoTriggerMsg) {
        try {
            if (this.offerMsg(recoTriggerMsg, "by human")) {
                return "success";
            }
        } catch (Exception e) {
            LOGGER.error("sendTriggerRecoMsg has error!", e);
        }
        return "failure";
    }

    public Boolean handleUpdateCheckRecoData(RecoStatusPo po) {
        if (po != null && reconStatusDao != null) {
            try {
                reconStatusDao.updateRecoStatus(po);
            } catch (Exception e) {
                LOGGER.error("handleUpdateCheckRecoData has exception ", e);
            }
        }
        return true;
    }

    public Boolean addMsgToTimeWheel(RecoStatusPo recoStatusPo) {
        if (timeWheel != null) {
            int retryTime = recoStatusPo.getRetryTime();
            if (retryTime == 0) {

            } else if (retryTime <= maxDelayTime) {
                return timeWheel.offerMsg(recoStatusPo, timerWheelDelayTimeRange[(retryTime - 1) % rangeSize]);
            }
        }
        return true;
    }

    @Override
    public long updateCheckTotalMsgNum(int num) {
        return this.totalCheckMsgNum.addAndGet(num);
    }

    @Override
    public long updateTotalMsgNum(int num) {
        return this.totalMsgNum.addAndGet(num);
    }

    @Override
    public PageInfo<RecoStatusPo> queryAuditRecoStatus(String groupId, String streamId,
            Boolean isDelete, String partitionDay,
            String partitionValue,
            int auditStatus,
            int startIndex, int pageSize) {
        try {
            if (startIndex < 0) {
                startIndex = 0;
            }
            final short deleteParam = (isDelete == null ? (short) 0 : (isDelete ? (short) 1 : (short) 0));
            PageHelper.startPage(startIndex, pageSize, "partition_value asc");
            List<RecoStatusPo> recoStatusPoList = this.reconStatusDao.selectRecoData(groupId, streamId, deleteParam,
                    String.valueOf(auditStatus), partitionDay, partitionValue);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("queryAuditRecoStatus groupId = {}, streamId = {}, isDelete = {}, "
                        + "partitionDay = {}, partitionValue = {}, auditStatus = {}, startIndex = {}, pageSize = {}, "
                        + "recoStatusPoList = {}", groupId, streamId,
                        isDelete, partitionDay, partitionValue, auditStatus, startIndex, pageSize,
                        StringUtils.join(recoStatusPoList, ","));
            }
            return PageInfo.of(recoStatusPoList);
        } catch (Exception e) {
            LOGGER.error("queryAuditRecoStatus has exception ", e);
        }
        return null;
    }

    private RecoStatusPo convertMsgToPo(RecoTriggerMsg recoTriggerMsg, String msgId) {
        RecoStatusPo.RecoStatusPoBuilder builder = RecoStatusPo.builder();
        String checkRatioStr = recoTriggerMsg.getCheckRatio();
        double ckRatio = recoCheckRatio;
        if (checkRatioStr != null) {
            ckRatio = Double.parseDouble(checkRatioStr);
        }
        String cycleNumberStr = recoTriggerMsg.getCycleNumber();
        Integer cycleNumber = null;
        if (StringUtils.isNotEmpty(cycleNumberStr) && StringUtils.isNumeric(cycleNumberStr)) {
            cycleNumber = Integer.parseInt(cycleNumberStr);
        }

        return builder.topoName(recoTriggerMsg.getTopoName())
                .inLongGroupId(recoTriggerMsg.getInLongGroupId())
                .inLongStreamId(recoTriggerMsg.getInLongStreamId())
                .partitionValue(getPartitionValue(recoTriggerMsg.getPartitionStartMs(),
                        recoTriggerMsg.getCycleUnit()))
                .partitionDay(getPartitionDay(recoTriggerMsg.getPartitionStartMs(),
                        recoTriggerMsg.getCycleUnit()))
                .usTaskId(recoTriggerMsg.getUsTaskId())
                .auditSourceId(recoTriggerMsg.getAuditSourceId())
                .auditSinkId(recoTriggerMsg.getAuditSinkId())
                .auditSinkAbandonId(recoTriggerMsg.getAuditSinkAbandonId())
                .auditTag(recoTriggerMsg.getAuditTag())
                .auditStatus(String.valueOf(RecoStatus.RETRY.code()))
                .auditType(recoTriggerMsg.getAuditType())
                .sinkType(recoTriggerMsg.getSinkType())
                .cycleUnit(recoTriggerMsg.getCycleUnit())
                .receiveMsgId(msgId)
                .sendMsgId("")
                .database(recoTriggerMsg.getDatabase())
                .table(recoTriggerMsg.getTable())
                .cycleNumber(cycleNumber)
                .partitionStartMs(recoTriggerMsg.getPartitionStartMs())
                .auditSourceCnt(-1)
                .auditSinkCnt(-1)
                .auditSinkAbandonCnt(-1)
                .retryTime(0)
                .isDelete((short) 0)
                .maxRetryTime(recoTriggerMsg.getMaxRecoRetryTime())
                .checkRatio(ckRatio)
                .msgHandlerTag(handleTag)
                .build();
    }

    private void printTriggerRecoWaterLog(RecoTriggerMsg recoTriggerMsg, String MsgId) {
        StringBuilder builder = new StringBuilder();
        builder.append(recoTriggerMsg.getTopoName()).append("|")
                .append(recoTriggerMsg.getInLongGroupId()).append("|")
                .append(recoTriggerMsg.getInLongStreamId()).append("|")
                .append(recoTriggerMsg.getAuditSourceId()).append("|")
                .append(recoTriggerMsg.getAuditSinkId()).append("|")
                .append(recoTriggerMsg.getAuditSinkAbandonId()).append("|")
                .append(recoTriggerMsg.getAuditTag()).append("|")
                .append(recoTriggerMsg.getAuditType()).append("|")
                .append(recoTriggerMsg.getSinkType()).append("|")
                .append(recoTriggerMsg.getDatabase()).append("|")
                .append(recoTriggerMsg.getTable()).append("|")
                .append(recoTriggerMsg.getUsTaskId()).append("|")
                .append(recoTriggerMsg.getPartitionStartMs()).append("|")
                .append(recoTriggerMsg.getPartitionEndMs()).append("|")
                .append(recoTriggerMsg.getCycleUnit()).append("|")
                .append(recoTriggerMsg.getCycleNumber()).append("|")
                .append(recoTriggerMsg.getMaxRecoRetryTime()).append("|")
                .append(recoTriggerMsg.getCheckRatio()).append("|")
                .append(MsgId);
        RECO_TRIGGER_WATER_LOGGER.info(builder.toString());
    }

    private String getPartitionDay(long pBeginTime, String pCycle) {
        return DateUtils.PARTITION_DAY_FORMATTER.format(DateUtils.getBeginDate(new Date(pBeginTime), pCycle));
    }

    private String getPartitionValue(long pBeginTime, String pCycle) {
        return DateUtils.PARTITION_VALUE_TIME_FORMATTER.format(DateUtils.getBeginDate(new Date(pBeginTime), pCycle));
    }

    private void initRecoTriggerMsg(RecoTriggerMsg recoTriggerMsg) {
        if (StringUtils.isEmpty(recoTriggerMsg.getAuditType())) {
            recoTriggerMsg.setAuditType("InLongAudit");
        }
        if (recoTriggerMsg.getMaxRecoRetryTime() == null) {
            recoTriggerMsg.setMaxRecoRetryTime(rangeSize);
        }
        if (recoTriggerMsg.getCheckRatio() == null) {
            recoTriggerMsg.setCheckRatio(String.valueOf(recoCheckRatio));
        }
    }

    public void close() throws IOException {
        isStopped.set(true);
        if (dispatchThread != null) {
            dispatchThread.setStopped();
        }
        if (checkThreadArray != null) {
            for (CheckThread checkThread : checkThreadArray) {
                checkThread.setStopped();
            }
        }
    }

    public void printMetric() {
        String log = "TotalMsg:" + totalMsgNum.get()
                + ",DispatchQueueSize=" + dispatchQueue.size()
                + ",TimeWheelSize=" + timeWheel.getTotalMsgNum()
                + ",TotalCheckMsgNum=" + totalCheckMsgNum.get();
        RECO_SYS_MONITOR_LOGGER.info(log);
    }
}
