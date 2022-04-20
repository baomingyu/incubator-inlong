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

package org.apache.inlong.manager.service.core;

import com.github.pagehelper.PageInfo;
import org.apache.inlong.manager.common.pojo.heartbeat.ComponentHeartBeatResponse;
import org.apache.inlong.manager.common.pojo.heartbeat.GroupHeartbeatResponse;
import org.apache.inlong.manager.common.pojo.heartbeat.HeartbeatReportRequest;
import org.apache.inlong.manager.common.pojo.heartbeat.StreamHeartBeatResponse;

public interface HeartbeatService {

    String reportHeartbeatInfo(HeartbeatReportRequest request);

    ComponentHeartBeatResponse getComponentHeartbeatInfo(String component,
            String instance);

    GroupHeartbeatResponse getGroupHeartbeatInfo(String component,
            String instance, String inlongGroupId);

    StreamHeartBeatResponse getStreamHeartbeatInfo(String component,
            String instance, String inlongGroupId, String inlongStreamId);

    PageInfo<ComponentHeartBeatResponse> getComponentHeartbeatInfos(String component, int pageNum,
            int pageSize);

    PageInfo<GroupHeartbeatResponse> getGroupHeartbeatInfos(String component,
            String instance, int pageNum, int pageSize);

    PageInfo<StreamHeartBeatResponse> getStreamHeartbeatInfos(String component,
            String instance, String inlongGroupId, int pageNum, int pageSize);

}
