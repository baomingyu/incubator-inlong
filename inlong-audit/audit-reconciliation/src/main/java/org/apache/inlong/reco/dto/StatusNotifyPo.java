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

package org.apache.inlong.reco.dto;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Data;

import javax.annotation.Nonnull;

import java.util.Date;

@Data
@Builder
public class StatusNotifyPo {

    @JsonProperty("partition")
    @Nonnull
    private final String partition;

    @JsonIgnore
    private final String partitionDay;

    /* for compatibility */
    @JsonProperty("usTaskId")
    @Nonnull
    private final String usTaskId;

    @JsonProperty("timestamp")
    @Nonnull
    private final String timestamp;

    @JsonProperty("topoName")
    private String topoName;

    @JsonProperty("inLongGroupId")
    private String inLongGroupId;

    @JsonProperty("inLongStreamId")
    private String inLongStreamId;

    @JsonProperty("database")
    private String database;

    @JsonProperty("table")
    private String table;

    @JsonProperty("isDelete")
    @JsonIgnore
    private short isDelete;

    @JsonProperty("createTime")
    @JsonIgnore
    private Date createTime;

    @JsonProperty("modifyTime")
    @JsonIgnore
    private Date modifyTime;

    @JsonProperty("sendMsgId")
    @JsonIgnore
    private String sendMsgId;
}
