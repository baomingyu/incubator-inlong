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

package org.apache.inlong.manager.service.core.impl;

import com.github.pagehelper.Page;
import com.github.pagehelper.PageHelper;
import com.github.pagehelper.PageInfo;
import com.google.gson.Gson;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.common.enums.ComponentTypeEnum;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.pojo.heartbeat.ComponentHeartBeatResponse;
import org.apache.inlong.manager.common.pojo.heartbeat.GroupHeartBeat;
import org.apache.inlong.manager.common.pojo.heartbeat.GroupHeartbeatResponse;
import org.apache.inlong.manager.common.pojo.heartbeat.HeartbeatReportRequest;
import org.apache.inlong.manager.common.pojo.heartbeat.StreamHeartBeat;
import org.apache.inlong.manager.common.pojo.heartbeat.StreamHeartBeatResponse;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.dao.entity.ComponentHeartbeatEntityWithBLOBs;
import org.apache.inlong.manager.dao.entity.GroupHeartbeatEntityWithBLOBs;
import org.apache.inlong.manager.dao.entity.StreamHeartbeatEntityWithBLOBs;
import org.apache.inlong.manager.dao.mapper.ComponentHeartbeatEntityMapper;
import org.apache.inlong.manager.dao.mapper.GroupHeartbeatEntityMapper;
import org.apache.inlong.manager.dao.mapper.StreamHeartbeatEntityMapper;
import org.apache.inlong.manager.service.core.HeartbeatService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 *  report or query heartbeat info
 */
@Service
public class HeartbeatServiceImpl
        implements HeartbeatService {

    private static final Logger LOGGER = LoggerFactory.getLogger(HeartbeatServiceImpl.class);

    @Autowired
    private ComponentHeartbeatEntityMapper componentHeartbeatEntityMapper;

    @Autowired
    private GroupHeartbeatEntityMapper groupHeartbeatEntityMapper;

    @Autowired
    private StreamHeartbeatEntityMapper streamHeartbeatEntityMapper;

    private static Gson gson = new Gson();

    /**
     * heartbeat common handler
     * @param request request
     * @return
     */
    @Override
    public String reportHeartbeatInfo(HeartbeatReportRequest request) {
        if (request != null && StringUtils.isNotEmpty(request.getComponent())) {
            ComponentTypeEnum componentType =
                    ComponentTypeEnum.valueOf(request.getComponent());
            switch (componentType) {
                case Sort:
                case DataProxy:
                case Agent:
                case Cache:
                default:
                    updateByDefaultWay(request);
            }
        } else {
            LOGGER.warn("request is null or component [{}] is not supported",
                    request.getComponent());
        }
        return "Success";
    }

    /**
     * update Heartbeat Data
     * @param request  request
     */
    private void updateComponentHeartbeatData(HeartbeatReportRequest request) {
        if (request == null || request.getComponentHeartBeat() == null) {
            return;
        }
        ComponentHeartbeatEntityWithBLOBs entity = new ComponentHeartbeatEntityWithBLOBs();
        entity.setComponent(request.getComponent());
        entity.setInstance(request.getInstance());
        entity.setReportTime(new Date(request.getReportTimestamp()));
        if (StringUtils.isNotEmpty(request.getComponentHeartBeat().getStatusHeartbeat())) {
            entity.setStatusHeartbeat(request.getComponentHeartBeat().getStatusHeartbeat());
        }
        if (StringUtils.isNotEmpty(request.getComponentHeartBeat().getMetricHeartbeat())) {
            entity.setMetricHeartbeat(request.getComponentHeartBeat().getMetricHeartbeat());
        }
        int count = componentHeartbeatEntityMapper.updateByKeyWithBLOBs(entity);
        if (count == 0) {
            componentHeartbeatEntityMapper.insert(entity);
        }
    }

    /**
     * update Component StaticData
     * @param request request
     */
    private void updateGroupHeartbeatData(HeartbeatReportRequest request) {
        List<GroupHeartBeat> list = request.getGroupHeartBeats();
        if (list != null) {
            for (GroupHeartBeat info : list) {
                GroupHeartbeatEntityWithBLOBs entity = new GroupHeartbeatEntityWithBLOBs();
                entity.setComponent(request.getComponent());
                entity.setInstance(request.getInstance());
                entity.setReportTime(new Date(request.getReportTimestamp()));
                entity.setInlongGroupId(info.getInlongGroupId());
                entity.setMetricHeartbeat(info.getMetricHeartbeat());
                entity.setStatusHeartbeat(info.getStatusHeartbeat());
                int count = groupHeartbeatEntityMapper.updateByKeyWithBLOBs(entity);
                if (count == 0) {
                    groupHeartbeatEntityMapper.insert(entity);
                }
            }
        }
    }

    /**
     * update Stream Status Data
     * @param request request
     */
    private void updateStreamHeartBeatData(HeartbeatReportRequest request) {
        List<StreamHeartBeat> list = request.getStreamHeartBeats();
        if (list != null) {
            for (StreamHeartBeat info : list) {
                StreamHeartbeatEntityWithBLOBs entity = new StreamHeartbeatEntityWithBLOBs();
                entity.setComponent(request.getComponent());
                entity.setInstance(request.getInstance());
                entity.setReportTime(new Date(request.getReportTimestamp()));
                entity.setInlongGroupId(info.getInlongGroupId());
                entity.setInlongStreamId(info.getInlongStreamId());
                entity.setMetricHeartbeat(info.getMetricHeartbeat());
                entity.setStatusHeartbeat(info.getStatusHeartbeat());
                int count = streamHeartbeatEntityMapper.updateByKeyWithBLOBs(entity);
                if (count == 0) {
                    streamHeartbeatEntityMapper.insert(entity);
                }
            }
        }
    }

    /**
     * get heartbeat info
     * @param component componentTypeName
     * @param instance instanceIdentifier
     * @return heartbeat info
     */
    @Override
    public ComponentHeartBeatResponse getComponentHeartbeatInfo(String component,
            String instance) {
        if (component != null && StringUtils.isNotEmpty(component)) {
            ComponentTypeEnum componentType =
                    ComponentTypeEnum.valueOf(component);
            switch (componentType) {
                case Sort:
                case DataProxy:
                case Agent:
                case Cache:
                default:
                    return getComponentHeartbeatInfoByDefaultWay(component, instance);
            }
        } else {
            LOGGER.warn("request is null or component type is null");
        }
        return null;
    }

    /**
     * get heartbeat static info
     * @param component componentTypeName
     * @param instance instanceIdentifier
     * @param inlongGroupId inlongGroupId
     * @return heartbeatStaticInfoResponse
     */
    @Override
    public GroupHeartbeatResponse getGroupHeartbeatInfo(String component,
            String instance, String inlongGroupId) {
        if (component != null && StringUtils.isNotEmpty(component)) {
            ComponentTypeEnum componentType =
                    ComponentTypeEnum.valueOf(component);
            switch (componentType) {
                case Sort:
                case DataProxy:
                case Agent:
                case Cache:
                default:
                    return getGroupHeartbeatByDefaultWay(component, instance, inlongGroupId);
            }
        } else {
            LOGGER.warn("request is null or component type is null");
        }
        return null;
    }

    /**
     * get heartbeat status info
     * @param component componentTypeName
     * @param instance instanceIdentifier
     * @param inlongGroupId inlongGroupId
     * @param inlongStreamId inlongStreamId
     * @return heartbeatStatusInfoResponse
     */
    @Override
    public StreamHeartBeatResponse getStreamHeartbeatInfo(String component,
            String instance, String inlongGroupId, String inlongStreamId) {
        if (component != null && StringUtils.isNotEmpty(component)) {
            ComponentTypeEnum componentType =
                    ComponentTypeEnum.valueOf(component);
            switch (componentType) {
                case Sort:
                case DataProxy:
                case Agent:
                case Cache:
                default:
                    return getStreamHeartbeatByDefaultWay(component, instance,
                            inlongGroupId, inlongStreamId);
            }
        } else {
            LOGGER.warn("request is null or component type is null");
        }
        return null;
    }

    /**
     * get component heartbeat infos
     * @param component  component
     * @param pageNum pageNum
     * @param pageSize pageSize
     * @return pageInfos
     */
    @Override
    public PageInfo<ComponentHeartBeatResponse> getComponentHeartbeatInfos(String component,
            int pageNum, int pageSize) {
        if (component != null && StringUtils.isNotEmpty(component)) {
            ComponentTypeEnum componentType =
                    ComponentTypeEnum.valueOf(component);
            switch (componentType) {
                case Sort:
                case DataProxy:
                case Agent:
                case Cache:
                default:
                    return getComponentHeartbeatInfosByDefaultWay(component, pageNum, pageSize);
            }
        } else {
            LOGGER.warn("request is null or component type is null");
        }
        return null;
    }

    /**
     * get group heartbeat infos
     * @param component component
     * @param instance instance
     * @param pageNum pageNum
     * @param pageSize pageSize
     * @return pageInfo
     */
    @Override
    public PageInfo<GroupHeartbeatResponse> getGroupHeartbeatInfos(String component,
            String instance, int pageNum, int pageSize) {
        if (component != null && StringUtils.isNotEmpty(component)) {
            ComponentTypeEnum componentType =
                    ComponentTypeEnum.valueOf(component);
            switch (componentType) {
                case Sort:
                case DataProxy:
                case Agent:
                case Cache:
                default:
                    return getGroupHeartbeatsByDefaultWay(component, instance,
                            pageNum, pageSize);
            }
        } else {
            LOGGER.warn("request is null or component type is null");
        }
        return null;
    }

    /**
     * get stream heartbeat infos
     * @param component component
     * @param instance instance
     * @param inlongGroupId inlongGroupId
     * @param pageNum pageNum
     * @param pageSize pageSize
     * @return pageInfo
     */
    @Override
    public PageInfo<StreamHeartBeatResponse> getStreamHeartbeatInfos(String component,
            String instance, String inlongGroupId, int pageNum, int pageSize) {
        if (component != null && StringUtils.isNotEmpty(component)) {
            ComponentTypeEnum componentType =
                    ComponentTypeEnum.valueOf(component);
            switch (componentType) {
                case Sort:
                case DataProxy:
                case Agent:
                case Cache:
                default:
                    return getStreamHeartbeatsByDefaultWay(component, instance,
                            inlongGroupId, pageNum, pageSize);
            }
        } else {
            LOGGER.warn("request is null or component type is null");
        }
        return null;
    }

    /**
     * update By DefaultWay
     * @param request request
     */
    private void updateByDefaultWay(HeartbeatReportRequest request) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("InlongHeartbeatReportRequest json = {}", gson.toJson(request));
        }
        updateComponentHeartbeatData(request);
        updateGroupHeartbeatData(request);
        updateStreamHeartBeatData(request);
    }

    /**
     * get Heartbeat InfoByDefaultWay
     * @param component component
     * @param instance instance
     * @return HeartbeatInfoResponse
     */
    private ComponentHeartBeatResponse getComponentHeartbeatInfoByDefaultWay(String component,
            String instance) {
        ComponentHeartbeatEntityWithBLOBs result =
                componentHeartbeatEntityMapper.selectByKey(component, instance);
        ComponentHeartBeatResponse componentHeartBeatResponse = null;
        if (result != null) {
            componentHeartBeatResponse = new ComponentHeartBeatResponse();
            componentHeartBeatResponse.setComponent(result.getComponent());
            componentHeartBeatResponse.setInstance(result.getInstance());
            componentHeartBeatResponse.setMetricHeartbeat(result.getMetricHeartbeat());
            componentHeartBeatResponse.setStatusHeartbeat(result.getStatusHeartbeat());
            componentHeartBeatResponse.setReportTime(result.getReportTime().getTime());
        }
        return componentHeartBeatResponse;
    }

    /**
     * get heartbeat StaticInfo ByDefaultWay
     * @param component component
     * @param instance instance
     * @param inlongGroupId inlongGroupId
     * @return heartbeatStaticInfoResponse
     */
    private GroupHeartbeatResponse getGroupHeartbeatByDefaultWay(String component,
            String instance, String inlongGroupId) {

        GroupHeartbeatEntityWithBLOBs result =
                groupHeartbeatEntityMapper.selectByKey(component,
                        instance, inlongGroupId);
        GroupHeartbeatResponse groupHeartbeatResponse = null;
        if (result != null) {
            groupHeartbeatResponse = new GroupHeartbeatResponse();
            groupHeartbeatResponse.setInlongGroupId(inlongGroupId);
            groupHeartbeatResponse.setComponent(result.getComponent());
            groupHeartbeatResponse.setInstance(result.getInstance());
            groupHeartbeatResponse.setReportTime(result.getReportTime().getTime());
            groupHeartbeatResponse.setMetricHeartbeat(result.getMetricHeartbeat());
            groupHeartbeatResponse.setStatusHeartbeat(result.getStatusHeartbeat());
        }
        return groupHeartbeatResponse;
    }

    /**
     * get Heartbeat StatusInfo ByDefaultWay
     * @param componentTypeName componentTypeName
     * @param instanceIdentifier instanceIdentifier
     * @param inlongGroupId inlongGroupId
     * @return heartbeatStatusInfoResponse
     */
    private StreamHeartBeatResponse getStreamHeartbeatByDefaultWay(String componentTypeName,
            String instanceIdentifier, String inlongGroupId, String inlongStreamId) {
        StreamHeartbeatEntityWithBLOBs result =
                streamHeartbeatEntityMapper.selectByKey(componentTypeName,
                        instanceIdentifier, inlongGroupId, inlongStreamId);
        StreamHeartBeatResponse streamHeartBeatResponse = null;
        if (result != null) {
            streamHeartBeatResponse = new StreamHeartBeatResponse();
            streamHeartBeatResponse.setComponent(result.getComponent());
            streamHeartBeatResponse.setInstance(result.getInstance());
            streamHeartBeatResponse.setReportTime(result.getReportTime().getTime());
            streamHeartBeatResponse.setInlongGroupId(result.getInlongGroupId());
            streamHeartBeatResponse.setInlongStreamId(result.getInlongStreamId());
            streamHeartBeatResponse.setMetricHeartbeat(result.getMetricHeartbeat());
            streamHeartBeatResponse.setStatusHeartbeat(result.getStatusHeartbeat());
        }
        return streamHeartBeatResponse;
    }

    /**
     * get Heartbeat InfoByDefaultWay
     * @param component component
     * @param pageNum pageNum
     * @param pageSize pageSize
     * @return HeartbeatInfoResponse
     */
    private PageInfo<ComponentHeartBeatResponse> getComponentHeartbeatInfosByDefaultWay(String component,
            int pageNum, int pageSize) {
        Preconditions.checkNotNull(component, ErrorCodeEnum.REQUEST_COMPONENT_EMPTY.getMessage());
        PageHelper.startPage(pageNum, pageSize);

        Page<ComponentHeartbeatEntityWithBLOBs> entityPage = (Page<ComponentHeartbeatEntityWithBLOBs>)
                componentHeartbeatEntityMapper.selectHeartBeats(component);

        List<ComponentHeartBeatResponse> componentHeartBeatResponses = new ArrayList<>();
        PageInfo<ComponentHeartBeatResponse> pageInfo;
        if (entityPage != null) {
            componentHeartBeatResponses = CommonBeanUtils
                    .copyListProperties(entityPage, ComponentHeartBeatResponse::new);
        }
        pageInfo = new PageInfo<>(componentHeartBeatResponses);
        pageInfo.setTotal(entityPage == null ? 0 : entityPage.getTotal());
        return pageInfo;
    }

    /**
     * get heartbeat StaticInfo ByDefaultWay
     * @param component component
     * @param instance instance
     * @param pageNum pageNum
     * @param pageSize pageSize
     * @return heartbeatStaticInfoResponse
     */
    private PageInfo<GroupHeartbeatResponse> getGroupHeartbeatsByDefaultWay(String component,
            String instance, int pageNum, int pageSize) {
        Preconditions.checkNotNull(component, ErrorCodeEnum.REQUEST_COMPONENT_EMPTY.getMessage());
        Preconditions.checkNotNull(component, ErrorCodeEnum.REQUEST_INSTANCE_EMPTY.getMessage());
        PageHelper.startPage(pageNum, pageSize);
        Page<GroupHeartbeatEntityWithBLOBs> entityPage = (Page<GroupHeartbeatEntityWithBLOBs>)
                groupHeartbeatEntityMapper.selectHeartBeats(component, instance);
        List<GroupHeartbeatResponse>  groupHeartbeatResponses = new ArrayList<>();
        PageInfo<GroupHeartbeatResponse> pageInfo;
        if (entityPage != null) {
            groupHeartbeatResponses = CommonBeanUtils
                    .copyListProperties(entityPage, GroupHeartbeatResponse::new);
        }
        pageInfo = new PageInfo<>(groupHeartbeatResponses);
        pageInfo.setTotal(entityPage == null ? 0 : entityPage.getTotal());
        return pageInfo;
    }

    /**
     * get Heartbeat StatusInfo ByDefaultWay
     * @param component component
     * @param instance instance
     * @param inlongGroupId inlongGroupId
     * @param pageNum pageNum
     * @param pageSize pageSize
     * @return heartbeatStatusInfoResponse
     */
    private PageInfo<StreamHeartBeatResponse> getStreamHeartbeatsByDefaultWay(String component,
            String instance, String inlongGroupId, int pageNum, int pageSize) {
        Preconditions.checkNotNull(component, ErrorCodeEnum.REQUEST_COMPONENT_EMPTY.getMessage());
        Preconditions.checkNotNull(instance, ErrorCodeEnum.REQUEST_INSTANCE_EMPTY.getMessage());
        Preconditions.checkNotNull(inlongGroupId, ErrorCodeEnum.GROUP_ID_IS_EMPTY.getMessage());
        PageHelper.startPage(pageNum, pageSize);
        Page<StreamHeartbeatEntityWithBLOBs> entityPage = (Page<StreamHeartbeatEntityWithBLOBs>)
                streamHeartbeatEntityMapper.selectHeartBeats(component, instance, inlongGroupId);
        List<StreamHeartBeatResponse>  streamHeartBeatResponses = new ArrayList<>();
        PageInfo<StreamHeartBeatResponse> pageInfo;
        if (entityPage != null) {
            streamHeartBeatResponses = CommonBeanUtils
                    .copyListProperties(entityPage, StreamHeartBeatResponse::new);
        }
        pageInfo = new PageInfo<>(streamHeartBeatResponses);
        pageInfo.setTotal(entityPage == null ? 0 : entityPage.getTotal());
        return pageInfo;
    }
}
