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
import org.apache.inlong.reco.service.RecoStatus;
import org.apache.inlong.reco.service.ReconciliationCheckService;
import org.apache.inlong.reco.util.DateUtils;

import com.github.pagehelper.PageInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.inlong.reco.util.DateUtils.PARTITION_VALUE_TIME_FORMATTER;

public class RecoveryThread extends Thread {

    private static Logger LOGGER = LoggerFactory.getLogger(RecoveryThread.class);

    private AtomicBoolean isStopped = new AtomicBoolean(false);

    private ReconciliationCheckService reconciliationCheckService;

    private int backRecoverDays;

    public RecoveryThread(ReconciliationCheckService reconciliationCheckService, int backRecoverDays) {
        this.reconciliationCheckService = reconciliationCheckService;
        this.backRecoverDays = backRecoverDays;
    }

    public void run() {
        try {
            Date date = new Date();
            long totalRecovery = 0L;
            for (int i = 0; i <= backRecoverDays; i++) {
                Calendar calendar = Calendar.getInstance();
                calendar.setTime(date);
                calendar.add(Calendar.DATE, (0 - i));
                String partitionDay = DateUtils.PARTITION_DAY_FORMATTER
                        .format(LocalDateTime.ofInstant(calendar.getTime().toInstant(),
                                ZoneId.systemDefault()));
                totalRecovery += recoveryOnePeriodData(partitionDay, RecoStatus.RETRY.code());
            }
            LOGGER.info("RecoveryThread Finished [{}], total recover number = {}, recovery Days = {}",
                    PARTITION_VALUE_TIME_FORMATTER.format(LocalDateTime.ofInstant(date.toInstant(),
                            ZoneId.systemDefault())),
                    totalRecovery, backRecoverDays);
            checkAndRedoData();
        } catch (Throwable e) {
            LOGGER.error("RecoveryThread has exception ", e);
        }
    }

    private long recoveryOnePeriodData(String partitionDay, int auditStatus) {
        int pageIndex = 0;
        int pageSize = 1000;
        long totalRecovery = 0L;
        boolean doRecovery = true;
        try {
            while (doRecovery && !isStopped.get()) {
                PageInfo<RecoStatusPo> pageInfo = reconciliationCheckService.queryAuditRecoStatus(null,
                        null, false, partitionDay, null, auditStatus,
                        pageIndex, pageSize);
                if (pageInfo != null && pageInfo.getList() != null) {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("PartitionDay = {} Total = {}, pageIndex = {}, pages = {}, "
                                + "pageResultSize = {}, auditStatus = {}", partitionDay,
                                pageInfo.getTotal(),
                                pageIndex, pageInfo.getPages(), pageInfo.getList().size(), auditStatus);
                    }

                    totalRecovery += pageInfo.getList().size();
                    if (pageInfo.getList().size() == 0) {
                        break;
                    }
                    List<RecoStatusPo> resultList = pageInfo.getList();
                    for (int j = 0; j < resultList.size(); j++) {
                        while (!reconciliationCheckService.handleRecoMsgData(resultList.get(j))
                                && !isStopped.get()) {
                            LOGGER.warn("recoveryOnePeriodData retry offer msg");
                            sleep(1000);
                        }
                    }
                    if (pageIndex < pageInfo.getPages()) {
                        pageIndex += 1;
                    } else {
                        break;
                    }
                } else {
                    LOGGER.warn("recoveryOnePeriodData [{}] page is null !", partitionDay);
                    break;
                }
            }
        } catch (Exception e) {
            LOGGER.error("recoveryOnePeriodData [{}] has exception ", partitionDay, e);
        }
        LOGGER.info("PartitionDay = {} Total = {}", partitionDay, totalRecovery);
        return totalRecovery;
    }

    private void checkAndRedoData() {

        while (!isStopped.get()) {
            Date date = new Date();
            long totalRecovery = 0L;
            for (int i = 0; i <= backRecoverDays; i++) {
                Calendar calendar = Calendar.getInstance();
                calendar.setTime(date);
                calendar.add(Calendar.DATE, (0 - i));
                String partitionDay = DateUtils.PARTITION_DAY_FORMATTER
                        .format(LocalDateTime.ofInstant(calendar.getTime().toInstant(),
                                ZoneId.systemDefault()));
                totalRecovery += recoveryOnePeriodData(partitionDay, RecoStatus.REDO.code());
            }
            try {
                sleep(10 * 60 * 1000L);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void setStopped() {
        isStopped.set(true);
    }
}
