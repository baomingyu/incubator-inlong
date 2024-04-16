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

package org.apache.inlong.reco.service.dispatch;

import org.apache.inlong.reco.dto.RecoStatusPo;
import org.apache.inlong.reco.dto.StatusNotifyPo;
import org.apache.inlong.reco.pulsar.PulsarProducerService;
import org.apache.inlong.reco.service.RecoStatus;
import org.apache.inlong.reco.service.ReconciliationCheckService;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class CheckThread extends Thread {

    private static Logger LOGGER = LoggerFactory.getLogger(CheckThread.class);

    private static Logger RECO_WATER_LOGGER = LoggerFactory.getLogger("RecoWaterLog");

    private final ObjectMapper jasonMapper = new ObjectMapper();

    private AtomicBoolean isStopped = new AtomicBoolean(false);

    private ArrayBlockingQueue<RecoStatusPo> queue;

    private ReconciliationCheckService reconciliationCheckService;

    private PulsarProducerService pulsarProducerService;

    private Semaphore semaphore;

    private final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public CheckThread(ReconciliationCheckService reconciliationCheckService, int queueSize,
            String pulsarBrokerUrl, String pulsarTopic) {
        this.reconciliationCheckService = reconciliationCheckService;
        this.queue = new ArrayBlockingQueue(queueSize);
        this.semaphore = new Semaphore(queueSize);
        this.pulsarProducerService = new PulsarProducerService(pulsarBrokerUrl, pulsarTopic);
    }

    public void run() {
        while (!isStopped.get()) {
            try {
                RecoStatusPo recoStatusPo = queue.poll(10, TimeUnit.SECONDS);
                if (recoStatusPo != null) {
                    String checkResult = recoStatusPo.getAuditStatus();
                    if (!String.valueOf(RecoStatus.SUCCESS.code()).equals(checkResult)) {
                        RecoStatus recoStatus = reconciliationCheckService.handleRecoCheck(recoStatusPo);
                        checkResult = (recoStatus != null ? String.valueOf(recoStatus.code()) : "3");
                    }

                    if (String.valueOf(RecoStatus.RETRY.code()).equals(checkResult)) {
                        if (recoStatusPo.getRetryTime() >= recoStatusPo.getMaxRetryTime()) {
                            recoStatusPo.setAuditStatus(String.valueOf(RecoStatus.FAILURE.code()));
                            reconciliationCheckService.handleUpdateCheckRecoData(recoStatusPo);
                        } else {
                            recoStatusPo.setRetryTime(recoStatusPo.getRetryTime() + 1);
                            recoStatusPo.setAuditStatus(String.valueOf(RecoStatus.RETRY.code()));
                            while (!isStopped.get() && !reconciliationCheckService.addMsgToTimeWheel(recoStatusPo)) {
                                LOGGER.warn("add msg to time wheel failure, and will retry later!");
                                sleep(1000);
                            }
                        }
                        semaphore.release();
                        reconciliationCheckService.updateCheckTotalMsgNum(-1);
                    } else if (checkResult == null || String.valueOf(RecoStatus.ABANDON.code()).equals(checkResult)) {
                        recoStatusPo.setAuditStatus(String.valueOf(RecoStatus.ABANDON.code()));
                        sendNotifyMsg(recoStatusPo);
                    } else if (String.valueOf(RecoStatus.SUCCESS.code()).equals(checkResult)) {
                        recoStatusPo.setAuditStatus(String.valueOf(RecoStatus.SUCCESS.code()));
                        sendNotifyMsg(recoStatusPo);
                    }
                    printRecoWaterLog(recoStatusPo);
                }
            } catch (Throwable e) {
                LOGGER.error("DispatchThread has exception ", e);
            }
        }
    }

    private void sendNotifyMsg(RecoStatusPo recoStatusPo) throws Exception {
        final String partition = simpleDateFormat.format(new Date(recoStatusPo.getPartitionStartMs()));
        StatusNotifyPo statusNotifyPo = StatusNotifyPo.builder()
                .partition(partition)
                .topoName(recoStatusPo.getTopoName())
                .inLongGroupId(recoStatusPo.getInLongGroupId())
                .inLongStreamId(recoStatusPo.getInLongStreamId())
                .timestamp(simpleDateFormat.format(new Date()))
                .usTaskId(recoStatusPo.getUsTaskId())
                .database(recoStatusPo.getDatabase())
                .table(recoStatusPo.getTable()).build();
        String msgId = pulsarProducerService.sendNotifyMsg(jasonMapper
                .writeValueAsString(statusNotifyPo).getBytes(StandardCharsets.UTF_8));
        if (StringUtils.isNotEmpty(msgId)) {
            recoStatusPo.setSendMsgId(msgId);
            reconciliationCheckService.handleUpdateCheckRecoData(recoStatusPo);
            reconciliationCheckService.updateTotalMsgNum(-1);
            reconciliationCheckService.updateCheckTotalMsgNum(-1);
            semaphore.release();
        } else {
            queue.put(recoStatusPo);
        }
    }

    private void printRecoWaterLog(RecoStatusPo recoStatusPo) {
        StringBuilder builder = new StringBuilder();
        builder.append(recoStatusPo.getTopoName()).append("|")
                .append(recoStatusPo.getInLongGroupId()).append("|")
                .append(recoStatusPo.getInLongStreamId()).append("|")
                .append(recoStatusPo.getAuditSourceId()).append("|")
                .append(recoStatusPo.getAuditSinkId()).append("|")
                .append(recoStatusPo.getAuditSinkAbandonId()).append("|")
                .append(recoStatusPo.getAuditTag()).append("|")
                .append(recoStatusPo.getAuditType()).append("|")
                .append(recoStatusPo.getUsTaskId()).append("|")
                .append(recoStatusPo.getPartitionStartMs()).append("|")
                .append(recoStatusPo.getPartitionValue()).append("|")
                .append(recoStatusPo.getRetryTime()).append("|")
                .append(recoStatusPo.getAuditSourceCnt()).append("|")
                .append(recoStatusPo.getAuditSinkCnt()).append("|")
                .append(recoStatusPo.getAuditSinkAbandonCnt()).append("|")
                .append(recoStatusPo.getAuditStatus()).append("|")
                .append(recoStatusPo.getReceiveMsgId()).append("|")
                .append(recoStatusPo.getSendMsgId());
        RECO_WATER_LOGGER.info(builder.toString());
    }

    public void setStopped() {
        isStopped.set(true);
    }

    public boolean offerMsg(RecoStatusPo po) {
        try {
            semaphore.tryAcquire(10, TimeUnit.MICROSECONDS);
            return this.queue.offer(po, 10, TimeUnit.MICROSECONDS);
        } catch (InterruptedException e) {
            LOGGER.error("offerMsg has exception", e);
        }
        return false;
    }
}
