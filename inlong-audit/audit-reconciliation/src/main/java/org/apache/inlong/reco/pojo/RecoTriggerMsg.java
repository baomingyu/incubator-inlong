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

package org.apache.inlong.reco.pojo;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Data;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

@Builder
@Data
public class RecoTriggerMsg {

    @JsonProperty("topoName")
    @Nullable
    private String topoName;

    @JsonProperty("inLongGroupId")
    @Nonnull
    private String inLongGroupId;

    @JsonProperty("inLongStreamId")
    @Nonnull
    private String inLongStreamId;

    @JsonProperty("partitionStartMs")
    @Nonnull
    private long partitionStartMs;

    @JsonProperty("partitionEndMs")
    @Nonnull
    private long partitionEndMs;

    @JsonProperty("cycleUnit")
    @Nonnull
    private String cycleUnit;

    @JsonProperty("cycleNumber")
    @Nonnull
    private String cycleNumber;

    @JsonProperty("auditSourceId")
    @Nonnull
    private String auditSourceId;

    @JsonProperty("auditSinkId")
    @Nonnull
    private String auditSinkId;

    @JsonProperty("auditSinkAbandonId")
    private String auditSinkAbandonId;

    @JsonProperty("auditTag")
    @Nullable
    private String auditTag;

    @JsonProperty("auditType")
    private String auditType;

    @JsonProperty("usTaskId")
    @Nonnull
    private String usTaskId;

    @JsonProperty("maxRecoRetryTime")
    @Nonnull
    private Integer maxRecoRetryTime = 12;

    @JsonProperty("database")
    @Nonnull
    private String database;

    @JsonProperty("table")
    @Nonnull
    private String table;

    @JsonProperty("sinkType")
    @Nonnull
    private String sinkType;

    @JsonProperty("checkRatio")
    @Nonnull
    private String checkRatio;
}
