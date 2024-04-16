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

package org.apache.inlong.reco.service.wheel;

import org.apache.inlong.reco.dto.RecoStatusPo;
import org.apache.inlong.reco.service.ReconciliationCheckService;
import org.apache.inlong.reco.service.factory.DefaultThreadFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class TimeWheel implements Runnable {

    private static Logger LOGGER = LoggerFactory.getLogger(TimeWheel.class);

    private int wheelSize;

    private int maxMsgCountPreQueue;

    private ArrayList<LinkedBlockingQueue<RecoStatusPo>> wheelArray = new ArrayList<>();

    private ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1,
            new DefaultThreadFactory("time-wheel-thread-"));

    private AtomicInteger currentIndex = new AtomicInteger(0);

    private ReconciliationCheckService reconciliationCheckService;

    private AtomicLong totalMsgNum = new AtomicLong(0L);

    public TimeWheel(int wheelSize, int tickM, int maxMsgCountPreQueue,
            ReconciliationCheckService reconciliationCheckService) {
        this.maxMsgCountPreQueue = maxMsgCountPreQueue;
        this.wheelSize = wheelSize;
        this.reconciliationCheckService = reconciliationCheckService;
        for (int i = 0; i < wheelSize; i++) {
            wheelArray.add(new LinkedBlockingQueue<RecoStatusPo>(maxMsgCountPreQueue));
        }
        executorService.scheduleWithFixedDelay(this, 0L,
                tickM, TimeUnit.MINUTES);
    }

    @Override
    public void run() {
        int index = Math.abs(currentIndex.getAndIncrement()) % wheelSize;
        LinkedBlockingQueue<RecoStatusPo> currentHandleQueue = wheelArray.get(index);
        LOGGER.info("TimeWheel index = {} current queue size = {}!",
                index, currentHandleQueue.size());
        try {
            while (true) {
                RecoStatusPo recoStatusPo = null;
                try {
                    recoStatusPo = currentHandleQueue.poll(10, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    LOGGER.warn("TimeWheel run has error!", e);
                }
                if (recoStatusPo != null) {
                    while (!reconciliationCheckService.handleRecoMsgData(recoStatusPo)) {
                        LOGGER.warn("TimeWheel retry offer msg");
                        Thread.sleep(1000);
                    }
                    totalMsgNum.addAndGet(-1);
                } else {
                    break;
                }
            }
        } catch (Exception e) {
            LOGGER.warn("TimeWheel run has error! index = {}", index, e);
        }
    }

    public boolean offerMsg(RecoStatusPo recoStatusPo, int delayTicks) {
        boolean offer = false;
        int curIndex = currentIndex.get();
        while (!offer) {
            int index = Math.abs(curIndex + delayTicks) % wheelSize;
            LinkedBlockingQueue currentHandleQueue = wheelArray.get(index);
            offer = currentHandleQueue.offer(recoStatusPo);
            if (offer) {
                totalMsgNum.addAndGet(1);
            } else {
                curIndex++;
                if (curIndex > (currentIndex.get() + wheelSize)) {
                    LOGGER.warn("wheel is full, and msg need handle slow!");
                    break;
                }
            }
        }
        return offer;
    }

    public long getTotalMsgNum() {
        return totalMsgNum.get();
    }
}
