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
import org.apache.inlong.reco.service.ReconciliationCheckService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class DispatchThread extends Thread {

    private static Logger LOGGER = LoggerFactory.getLogger(DispatchThread.class);

    private AtomicBoolean isStopped = new AtomicBoolean(false);

    private ArrayBlockingQueue<RecoStatusPo> queue;

    private ReconciliationCheckService reconciliationCheckService;

    public DispatchThread(ArrayBlockingQueue<RecoStatusPo> queue,
            ReconciliationCheckService reconciliationCheckService) {
        this.queue = queue;
        this.reconciliationCheckService = reconciliationCheckService;
    }

    public void run() {
        while (!isStopped.get() && queue != null) {
            try {
                RecoStatusPo po = queue.poll(10, TimeUnit.SECONDS);
                if (po != null) {
                    reconciliationCheckService.handleRecoMsgData(po);
                }
            } catch (Throwable e) {
                LOGGER.error("DispatchThread has exception ", e);
            }
        }
    }

    public void setStopped() {
        isStopped.set(true);
    }
}
