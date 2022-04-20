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

package org.apache.inlong.manager.web.controller;

import com.github.pagehelper.PageInfo;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.apache.inlong.manager.common.beans.Response;
import org.apache.inlong.manager.common.pojo.heartbeat.ComponentHeartBeatPageRequest;
import org.apache.inlong.manager.common.pojo.heartbeat.ComponentHeartBeatResponse;
import org.apache.inlong.manager.common.pojo.heartbeat.GroupHeartbeatPageRequest;
import org.apache.inlong.manager.common.pojo.heartbeat.GroupHeartbeatResponse;
import org.apache.inlong.manager.common.pojo.heartbeat.GroupHeartbeatRequest;
import org.apache.inlong.manager.common.pojo.heartbeat.ComponentHeartBeatRequest;
import org.apache.inlong.manager.common.pojo.heartbeat.StreamHeartBeatPageRequest;
import org.apache.inlong.manager.common.pojo.heartbeat.StreamHeartBeatRequest;
import org.apache.inlong.manager.common.pojo.heartbeat.StreamHeartBeatResponse;
import org.apache.inlong.manager.service.core.HeartbeatService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/heartbeat/")
@Api(tags = "Heartbeat")
public class HeartbeatInfoController {

    @Autowired
    private HeartbeatService heartbeatService;

    @RequestMapping(value = "/component/info", method = RequestMethod.POST)
    @ApiOperation(value = "query component heartbeat")
    public Response<ComponentHeartBeatResponse> queryComponentHeartbeatInfo(@RequestBody
            ComponentHeartBeatRequest info) {
        ComponentHeartBeatResponse response =
                heartbeatService.getComponentHeartbeatInfo(info.getComponent(),
                info.getInstance());
        if (response == null) {
            return Response.fail("Not found msg!");
        } else {
            return Response.success(response);
        }
    }

    @RequestMapping(value = "/group/info", method = RequestMethod.POST)
    @ApiOperation(value = "query group heartbeat")
    public Response<GroupHeartbeatResponse> queryGroupHeartbeatInfo(@RequestBody
            GroupHeartbeatRequest info) {
        GroupHeartbeatResponse response = heartbeatService
                .getGroupHeartbeatInfo(info.getComponent(), info.getInstance(),
                        info.getInlongGroupId());
        if (response == null) {
            return Response.fail("Not found msg!");
        } else {
            return Response.success(response);
        }
    }

    @RequestMapping(value = "/stream/info", method = RequestMethod.POST)
    @ApiOperation(value = "query stream heartbeat")
    public Response<StreamHeartBeatResponse> queryStreamHeartbeat(@RequestBody
            StreamHeartBeatRequest info) {
        StreamHeartBeatResponse response = heartbeatService
                .getStreamHeartbeatInfo(info.getComponent(),
                info.getInstance(), info.getInlongGroupId(), info.getInlongStreamId());
        if (response == null) {
            return Response.fail("Not found msg!");
        } else {
            return Response.success(response);
        }
    }

    @RequestMapping(value = "/component/infos", method = RequestMethod.POST)
    @ApiOperation(value = "query component heartbeats")
    public Response<PageInfo<ComponentHeartBeatResponse>> queryComponentHeartbeatInfos(@RequestBody
            ComponentHeartBeatPageRequest info) {
        PageInfo<ComponentHeartBeatResponse> responses =
                heartbeatService.getComponentHeartbeatInfos(info.getComponent(), info.getPageNum(),
                        info.getPageSize());
        if (responses == null) {
            return Response.fail("Not found msg!");
        } else {
            return Response.success(responses);
        }
    }

    @RequestMapping(value = "/group/infos", method = RequestMethod.POST)
    @ApiOperation(value = "query group heartbeats")
    public Response<PageInfo<GroupHeartbeatResponse>> queryGroupHeartbeatInfos(@RequestBody
            GroupHeartbeatPageRequest info) {
        PageInfo<GroupHeartbeatResponse> responses = heartbeatService
                .getGroupHeartbeatInfos(info.getComponent(), info.getInstance(),
                        info.getPageNum(), info.getPageSize());
        if (responses == null) {
            return Response.fail("Not found msg!");
        } else {
            return Response.success(responses);
        }
    }

    @RequestMapping(value = "/stream/infos", method = RequestMethod.POST)
    @ApiOperation(value = "query stream heartbeats")
    public Response<PageInfo<StreamHeartBeatResponse>> queryStreamHeartbeats(@RequestBody
            StreamHeartBeatPageRequest info) {
        PageInfo<StreamHeartBeatResponse> responses = heartbeatService
                .getStreamHeartbeatInfos(info.getComponent(), info.getInstance(),
                        info.getInlongGroupId(), info.getPageNum(), info.getPageSize());
        if (responses == null) {
            return Response.fail("Not found msg!");
        } else {
            return Response.success(responses);
        }
    }

}

