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

package org.apache.inlong.reco.util;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.Date;

/**
 * time tools
 */
public class DateUtils {

    public static DateTimeFormatter PARTITION_VALUE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");

    public static DateTimeFormatter US_PARTITION_VALUE_TIME_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public static DateTimeFormatter PARTITION_DAY_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd");

    public static final String CYCLE_UNIT_D = "D";
    public static final String CYCLE_UNIT_H = "H";
    public static final String CYCLE_UNIT_I = "I";

    public static Date getPartitionEndDate(Date date, String cycleUnit, int delayMinute) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);

        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);

        if (CYCLE_UNIT_D.equals(cycleUnit)) {
            calendar.add(Calendar.DAY_OF_MONTH, +1);
            calendar.set(Calendar.HOUR_OF_DAY, 0);
        } else if (CYCLE_UNIT_H.equals(cycleUnit)) {
            calendar.add(Calendar.HOUR_OF_DAY, +1);
        }

        delayMinute = (delayMinute < 0 || delayMinute > 59) ? 10 : delayMinute;
        calendar.set(Calendar.MINUTE, delayMinute);

        return calendar.getTime();
    }

    public static Date getPartitionStartDate(Date date, String cycleUnit) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);

        if (CYCLE_UNIT_H.equals(cycleUnit)) {
            calendar.set(Calendar.MINUTE, 0);
        } else if (CYCLE_UNIT_D.equals(cycleUnit)) {
            calendar.set(Calendar.HOUR_OF_DAY, 0);
        }
        return calendar.getTime();
    }

    public static LocalDateTime getBeginDate(Date taskCurRunDate, String cycleUnit) {
        Date pBeginDate = getPartitionStartDate(taskCurRunDate, cycleUnit);
        return LocalDateTime.ofInstant(pBeginDate.toInstant(), ZoneId.systemDefault());
    }

    public static LocalDateTime getEndData(Date taskCurRunDate, String cycleUnit, int cycleNumber) {
        Date pBeginDate = getPartitionStartDate(taskCurRunDate, cycleUnit);
        LocalDateTime beginDateLdt = LocalDateTime.ofInstant(pBeginDate.toInstant(),
                ZoneId.systemDefault());
        LocalDateTime endDateLdt = null;
        if ("I".equalsIgnoreCase(cycleUnit)) {
            endDateLdt = beginDateLdt.plusMinutes(cycleNumber);
        } else if ("H".equalsIgnoreCase(cycleUnit)) {
            endDateLdt = beginDateLdt.plusHours(cycleNumber);
        } else if ("D".equalsIgnoreCase(cycleUnit)) {
            endDateLdt = beginDateLdt.plusDays(cycleNumber);
        }
        return endDateLdt;
    }

}
