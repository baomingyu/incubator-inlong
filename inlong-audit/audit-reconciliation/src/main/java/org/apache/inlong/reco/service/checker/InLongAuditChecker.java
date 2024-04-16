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

package org.apache.inlong.reco.service.checker;

import org.apache.inlong.reco.dao.audit.InLongAuditDao;
import org.apache.inlong.reco.dto.RecoStatusPo;
import org.apache.inlong.reco.service.AuditCheckerService;
import org.apache.inlong.reco.util.DateUtils;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.Date;

@Service("InLongAudit")
public class InLongAuditChecker implements AuditCheckerService {

    private static final Logger LOGGER = LoggerFactory.getLogger(InLongAuditChecker.class);

    @Autowired
    private InLongAuditDao inLongAuditDao;

    @Override
    public boolean check(RecoStatusPo recoStatusPo) {
        Date taskRunDate = new Date(recoStatusPo.getPartitionStartMs());
        String cycleUnit = recoStatusPo.getCycleUnit();
        LocalDateTime beginDateLdt = DateUtils.getBeginDate(taskRunDate, cycleUnit);
        LocalDateTime endDateLdt = DateUtils.getEndData(taskRunDate, cycleUnit, recoStatusPo.getCycleNumber());
        String beginDateStr = DATA_TIME_FORMAT.format(beginDateLdt);
        String endDateStr = DATA_TIME_FORMAT.format(endDateLdt);
        Long sourceAuditCnt = getAuditMetric(recoStatusPo.getInLongStreamId(), recoStatusPo.getInLongGroupId(),
                recoStatusPo.getAuditSourceId(), recoStatusPo.getAuditTag(), beginDateStr, endDateStr);
        if (sourceAuditCnt != null && sourceAuditCnt >= 0) {
            recoStatusPo.setAuditSourceCnt(sourceAuditCnt);
        }

        Long sinkAuditCnt = getAuditMetric(recoStatusPo.getInLongStreamId(), recoStatusPo.getInLongGroupId(),
                recoStatusPo.getAuditSinkId(), recoStatusPo.getAuditTag(), beginDateStr, endDateStr);
        if (sinkAuditCnt != null && sinkAuditCnt >= 0) {
            recoStatusPo.setAuditSinkCnt(sinkAuditCnt);
        }
        Long sinkAuditAbandonCnt = getAuditMetric(recoStatusPo.getInLongStreamId(), recoStatusPo.getInLongGroupId(),
                recoStatusPo.getAuditSinkAbandonId(), recoStatusPo.getAuditTag(), beginDateStr, endDateStr);
        if (sinkAuditAbandonCnt != null && sinkAuditAbandonCnt >= 0) {
            recoStatusPo.setAuditSinkAbandonCnt(sinkAuditAbandonCnt);
        }

        double checkRatio = recoStatusPo.getCheckRatio();
        Long totalSinkCnt = null;
        if (sinkAuditCnt != null && sinkAuditAbandonCnt != null) {
            totalSinkCnt = sinkAuditCnt + sinkAuditAbandonCnt;
        } else {
            if (sinkAuditCnt != null) {
                totalSinkCnt = sinkAuditCnt;
            } else if (sinkAuditAbandonCnt != null) {
                totalSinkCnt = sinkAuditAbandonCnt;
            }
        }
        boolean cmpResult = false;
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("sourceAuditCnt=").append(sourceAuditCnt)
                .append(", sinkAuditCnt=").append(sinkAuditCnt)
                .append(", sinkAbandonCnt=").append(sinkAuditAbandonCnt);
        if (totalSinkCnt != null && sourceAuditCnt != null) {
            if (totalSinkCnt > sourceAuditCnt) {
                cmpResult = true;
            } else {
                double cmpRatio = calcMetricMatchRatio(totalSinkCnt, sourceAuditCnt);
                stringBuilder.append(", miss ratio=").append(cmpRatio).append(", ");
                cmpResult = (cmpRatio <= (1.0d - checkRatio));
            }
        }
        if (cmpResult) {
            stringBuilder.append("reconciliation successful!");
        } else {
            stringBuilder.append("reconciliation Failed!");
        }
        LOGGER.info(stringBuilder.toString());
        return cmpResult;
    }

    /**
     * 支持mysql、clickhouse
     */
    private Long getAuditMetric(String streamId, String groupId, String auditId, String auditTag,
            String beginDateStr, String endDateStr) {
        Long msgCount = null;
        try {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append("select sum(count) from audit_data where inlong_group_id = '")
                    .append(groupId).append("'")
                    .append(" and inlong_stream_id = '").append(streamId).append("'")
                    .append(" and audit_id = '").append(auditId).append("'");
            if (StringUtils.isNotEmpty(auditTag)) {
                stringBuilder.append(" and audit_tag = '").append(auditTag).append("'");
            }
            stringBuilder.append(" and log_ts >= '").append(beginDateStr).append("'")
                    .append(" and log_ts <'").append(endDateStr).append("';");
            LOGGER.info("SQL = " + stringBuilder);
            if (StringUtils.isNotEmpty(auditId)) {
                msgCount = inLongAuditDao.selectAuditCnt(groupId, streamId, auditId, auditTag,
                        beginDateStr, endDateStr);
            }
        } catch (Throwable e) {
            LOGGER.error("select cnt from mysql has exception! " + e);
        }
        return msgCount;
    }

    /**
     *
     * @param totalSinkCnt
     * @param sourceAuditCnt
     * @return
     */
    public double calcMetricMatchRatio(Long totalSinkCnt, Long sourceAuditCnt) {
        if (totalSinkCnt == null || sourceAuditCnt == null || totalSinkCnt < 0 || sourceAuditCnt < 0) {
            return 1d;
        }
        if (totalSinkCnt == sourceAuditCnt) {
            return 0d;
        }

        final double difference = Math.abs(totalSinkCnt - sourceAuditCnt);
        final long base = Math.max(totalSinkCnt, sourceAuditCnt);
        /*
         * should not happen, just a double check to avoid dividing 0
         */
        if (base == 0) {
            return 1d;
        }
        return difference / base;
    }
}