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

package org.apache.inlong.reco.controller;

import org.apache.inlong.reco.dto.RecoStatusPo;
import org.apache.inlong.reco.dto.StatusNotifyPo;
import org.apache.inlong.reco.pojo.RecoSendMsgRsp;
import org.apache.inlong.reco.pojo.RecoStatusNotifyMsg;
import org.apache.inlong.reco.pojo.RecoTriggerMsg;
import org.apache.inlong.reco.pojo.Response;
import org.apache.inlong.reco.service.RecoStatus;
import org.apache.inlong.reco.service.ReconciliationCheckService;
import org.apache.inlong.reco.util.DateUtils;

import com.github.pagehelper.PageInfo;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

@RestController
@RequestMapping("/audit/reconciliation")
@Api(tags = "InLong audit reconciliation controller")
public class PartitionToolsController {

    private static final Logger LOGGER = LoggerFactory.getLogger(PartitionToolsController.class);

    @Autowired
    private ReconciliationCheckService reconciliationCheckService;

    @RequestMapping(value = "/failure/partition/{groupId}/{streamId}/{partitionDay}", method = RequestMethod.GET)
    @ApiOperation(value = "get unclosed partition list of specify group id and streamId")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "groupId", dataTypeClass = String.class, required = true),
            @ApiImplicitParam(name = "streamId", dataTypeClass = String.class, required = true),
            @ApiImplicitParam(name = "partitionDay", dataTypeClass = String.class, required = true)})
    public Response<List<RecoStatusPo>> getFailureRecoPartitionNameList(@PathVariable String groupId,
            @PathVariable String streamId,
            @PathVariable String partitionDay) {
        LOGGER.info("getFailureRecoPartitionNameList groupId = {}, streamId = {}, partitionDay = {}",
                groupId, streamId, partitionDay);
        int pageIndex = 0;
        int pageSize = 1000;
        List<RecoStatusPo> resultList = new ArrayList<>();
        try {
            while (true) {
                PageInfo<RecoStatusPo> page = reconciliationCheckService.queryAuditRecoStatus(groupId, streamId,
                        false, partitionDay, null, RecoStatus.FAILURE.code(), pageIndex, pageSize);
                if (page != null && page.getList() != null) {
                    if (page.getList().size() == 0) {
                        break;
                    }
                    resultList.addAll(page.getList());
                    if (pageIndex < page.getPages()) {
                        pageIndex += 1;
                    } else {
                        break;
                    }
                } else {
                    LOGGER.warn("getFailureRecoPartitionNameList [{}] page is null !", partitionDay);
                }
            }
        } catch (Exception e) {
            LOGGER.error("getFailureRecoPartitionNameList [{}] has exception ", partitionDay, e);
        }
        return Response.success(resultList);
    }

    @RequestMapping(value = "/unclose/partition/{groupId}/{streamId}/{partitionDay}", method = RequestMethod.GET)
    @ApiOperation(value = "get unclosed partition list of specify group id and streamId")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "groupId", dataTypeClass = String.class, required = true),
            @ApiImplicitParam(name = "streamId", dataTypeClass = String.class, required = true),
            @ApiImplicitParam(name = "partitionDay", dataTypeClass = String.class, required = true)})
    public Response<List<RecoStatusPo>> getUnCloseRecoPartitionNameList(@PathVariable String groupId,
            @PathVariable String streamId,
            @PathVariable String partitionDay) {

        int pageIndex = 0;
        int pageSize = 1000;
        List<RecoStatusPo> resultList = new ArrayList<>();
        try {
            while (true) {
                PageInfo<RecoStatusPo> page = reconciliationCheckService.queryAuditRecoStatus(groupId, streamId,
                        false, partitionDay, null, RecoStatus.RETRY.code(), pageIndex, pageSize);
                if (page != null && page.getList() != null) {
                    if (page.getList().size() == 0) {
                        break;
                    }
                    resultList.addAll(page.getList());
                    if (pageIndex < page.getPages()) {
                        pageIndex += 1;
                    } else {
                        break;
                    }
                } else {
                    LOGGER.warn("getFailureRecoPartitionNameList [{}] page is null !", partitionDay);
                }
            }
        } catch (Exception e) {
            LOGGER.error("getFailureRecoPartitionNameList [{}] has exception ", partitionDay, e);
        }
        return Response.success(resultList);
    }

    @RequestMapping(value = "/failure/reco/{partitionDay}/{pageIndex}/{pageSize}", method = RequestMethod.GET)
    @ApiOperation(value = "get unclosed partition list of specify partition day")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "partitionDay", dataTypeClass = String.class, required = true),
            @ApiImplicitParam(name = "pageIndex", dataTypeClass = Integer.class, required = true),
            @ApiImplicitParam(name = "pageSize", dataTypeClass = Integer.class, required = true)})
    public Response<PageInfo<RecoStatusPo>> getFailureRecoPartitionNameList(@PathVariable String partitionDay,
            @PathVariable Integer pageIndex,
            @PathVariable Integer pageSize) {
        PageInfo<RecoStatusPo> page = reconciliationCheckService.queryAuditRecoStatus(null, null,
                false, partitionDay, null, RecoStatus.FAILURE.code(), pageIndex, pageSize);
        return Response.success(page);
    }

    @RequestMapping(value = "/unclose/reco/{partitionDay}/{pageIndex}/{pageSize}", method = RequestMethod.GET)
    @ApiOperation(value = "get unclosed partition list of specify partition day")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "partitionDay", dataTypeClass = String.class, required = true),
            @ApiImplicitParam(name = "pageIndex", dataTypeClass = Integer.class, required = true),
            @ApiImplicitParam(name = "pageSize", dataTypeClass = Integer.class, required = true)})
    public Response<PageInfo<RecoStatusPo>> getUnCloseRecoPartitionNameList(@PathVariable String partitionDay,
            @PathVariable Integer pageIndex,
            @PathVariable Integer pageSize) {

        PageInfo<RecoStatusPo> page = reconciliationCheckService.queryAuditRecoStatus(null, null,
                false, partitionDay, null, RecoStatus.RETRY.code(), pageIndex, pageSize);
        return Response.success(page);
    }

    @RequestMapping(value = "/send/close/partition/msg", method = RequestMethod.POST)
    @ApiOperation(value = "send close partition msg for reconciliation")
    public Response<RecoSendMsgRsp> sendMsgForReconciliation(@Validated @RequestBody RecoTriggerMsg recoTriggerMsg) {
        RecoSendMsgRsp recoSendMsgRsp =
                RecoSendMsgRsp.builder().msgId(reconciliationCheckService.sendTriggerRecoMsg(recoTriggerMsg))
                        .partition(recoTriggerMsg.getPartitionStartMs())
                        .inLongGroupId(recoTriggerMsg.getInLongGroupId())
                        .inLongStreamId(recoTriggerMsg.getInLongStreamId())
                        .build();
        return Response.success(recoSendMsgRsp);
    }

    @RequestMapping(value = "/send/us/partition/msg", method = RequestMethod.POST)
    @ApiOperation(value = "send us msg for us runner")
    public Response<RecoSendMsgRsp> sendUsMsgForUsRunner(
            @Validated @RequestBody RecoStatusNotifyMsg recoStatusNotifyMsg) {
        StatusNotifyPo statusNotifyPo = StatusNotifyPo.builder()
                .partition(DateUtils.US_PARTITION_VALUE_TIME_FORMATTER
                        .format(LocalDateTime.ofInstant(new Date(recoStatusNotifyMsg.getPartition()).toInstant(),
                                ZoneId.systemDefault())))
                .partitionDay(DateUtils.PARTITION_DAY_FORMATTER
                        .format(LocalDateTime.ofInstant(new Date(recoStatusNotifyMsg.getPartition()).toInstant(),
                                ZoneId.systemDefault())))
                .database(recoStatusNotifyMsg.getDatabase())
                .table(recoStatusNotifyMsg.getTable())
                .usTaskId(recoStatusNotifyMsg.getUsTaskId())
                .topoName(recoStatusNotifyMsg.getTopoName())
                .timestamp(DateUtils.US_PARTITION_VALUE_TIME_FORMATTER
                        .format(LocalDateTime.ofInstant(new Date().toInstant(), ZoneId.systemDefault())))
                .inLongGroupId(recoStatusNotifyMsg.getInLongGroupId())
                .inLongStreamId(recoStatusNotifyMsg.getInLongStreamId()).build();
        RecoSendMsgRsp recoSendMsgRsp =
                RecoSendMsgRsp.builder().msgId(reconciliationCheckService.sendRecoNotifyMsg(statusNotifyPo))
                        .partition(recoStatusNotifyMsg.getPartition())
                        .inLongGroupId(recoStatusNotifyMsg.getInLongGroupId())
                        .inLongStreamId(recoStatusNotifyMsg.getInLongStreamId())
                        .build();
        return Response.success(recoSendMsgRsp);
    }
}
