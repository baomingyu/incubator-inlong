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

import org.apache.inlong.reco.dto.RecoStatusPo;
import org.apache.inlong.reco.pojo.Response;
import org.apache.inlong.reco.service.AuditCheckerService;
import org.apache.inlong.reco.util.DateUtils;
import org.apache.inlong.reco.util.HttpUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;

@Service("AttaAudit")
public class AttaAuditChecker implements AuditCheckerService {

    private static final Logger LOGGER = LoggerFactory.getLogger(AttaAuditChecker.class);

    @Value("${inlong.audit.reconciliation.atta.base.url:''}")
    private String attaAuditBaseUrl;

    @Override
    public boolean check(RecoStatusPo recoStatusPo) {
        Date taskRunDate = new Date(recoStatusPo.getPartitionStartMs());
        try {
            /*
             * query check result form ledger service
             */
            String cycleUnit = recoStatusPo.getCycleUnit();
            Date beginDate = DateUtils.getPartitionStartDate(taskRunDate, cycleUnit);
            LocalDateTime beginDateLdt = LocalDateTime.ofInstant(beginDate.toInstant(),
                    ZoneId.systemDefault());
            String partition = AuditCheckerService.HOUR_FORMATTER.format(beginDateLdt);
            Response<Boolean> check = HttpUtils.checkAttaAuditFromLedger(attaAuditBaseUrl,
                    recoStatusPo.getInLongGroupId(), partition, "thive");
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Atta check recoStatusPo {}, check {}" + recoStatusPo, check);
            }
            if (check != null && check.getData()) {
                return check.getData();
            } else {
                return false;
            }
        } catch (Exception e) {
            LOGGER.error("Atta checker {} has exception ", recoStatusPo, e);
        }
        return false;
    }
}
