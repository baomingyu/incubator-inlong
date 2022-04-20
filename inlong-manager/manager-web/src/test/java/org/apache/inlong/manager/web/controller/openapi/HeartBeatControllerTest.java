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

package org.apache.inlong.manager.web.controller.openapi;

import static org.mockito.BDDMockito.given;

import com.github.pagehelper.Page;
import com.github.pagehelper.PageInfo;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.apache.inlong.manager.common.pojo.heartbeat.ComponentHeartBeat;
import org.apache.inlong.manager.common.pojo.heartbeat.ComponentHeartBeatResponse;
import org.apache.inlong.manager.common.pojo.heartbeat.GroupHeartBeat;
import org.apache.inlong.manager.common.pojo.heartbeat.GroupHeartbeatResponse;
import org.apache.inlong.manager.common.pojo.heartbeat.HeartbeatReportRequest;
import org.apache.inlong.manager.common.pojo.heartbeat.StreamHeartBeat;
import org.apache.inlong.manager.common.pojo.heartbeat.StreamHeartBeatResponse;
import org.apache.inlong.manager.dao.entity.ComponentHeartbeatEntityWithBLOBs;
import org.apache.inlong.manager.dao.entity.GroupHeartbeatEntityWithBLOBs;
import org.apache.inlong.manager.dao.entity.StreamHeartbeatEntityWithBLOBs;
import org.apache.inlong.manager.dao.mapper.ComponentHeartbeatEntityMapper;
import org.apache.inlong.manager.dao.mapper.GroupHeartbeatEntityMapper;
import org.apache.inlong.manager.dao.mapper.StreamHeartbeatEntityMapper;
import org.apache.inlong.manager.service.core.HeartbeatService;
import org.apache.inlong.manager.service.core.impl.HeartbeatServiceImpl;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
public class HeartBeatControllerTest {

    @InjectMocks
    private HeartbeatService heartbeatService = new HeartbeatServiceImpl();

    @Mock
    private ComponentHeartbeatEntityMapper componentHeartbeatEntityMapper;

    @Mock
    private GroupHeartbeatEntityMapper groupHeartbeatEntityMapper;

    @Mock
    private StreamHeartbeatEntityMapper streamHeartbeatEntityMapper;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);

        ComponentHeartbeatEntityWithBLOBs componentHeartbeatEntityWithBLOBs =
                new ComponentHeartbeatEntityWithBLOBs();
        componentHeartbeatEntityWithBLOBs.setComponent("Sort");
        componentHeartbeatEntityWithBLOBs.setInstance("127.0.0.1");
        componentHeartbeatEntityWithBLOBs.setStatusHeartbeat("[{\"inlongGroupId\":\"groupId\","
                + "\"componentStaticInfo\":\"\"}]");
        componentHeartbeatEntityWithBLOBs.setMetricHeartbeat("[{\"inlongGroupId\":\"groupId\","
                + "\"streamStatusInfo\":\"\"}]");
        componentHeartbeatEntityWithBLOBs.setReportTime(new Date());
        Page<ComponentHeartbeatEntityWithBLOBs> componentPage = new Page<>();
        componentPage.add(componentHeartbeatEntityWithBLOBs);
        componentPage.setTotal(1);
        given(componentHeartbeatEntityMapper.insert(new ComponentHeartbeatEntityWithBLOBs())).willReturn(1);
        given(componentHeartbeatEntityMapper.selectByKey(Mockito.anyString(),
                Mockito.anyString())).willReturn(componentHeartbeatEntityWithBLOBs);
        given(componentHeartbeatEntityMapper.selectHeartBeats(Mockito.anyString()))
                .willReturn(componentPage);

        GroupHeartbeatEntityWithBLOBs groupHeartbeatEntityWithBLOBs =
                new GroupHeartbeatEntityWithBLOBs();
        groupHeartbeatEntityWithBLOBs.setComponent("Sort");
        groupHeartbeatEntityWithBLOBs.setInstance("127.0.0.1");
        groupHeartbeatEntityWithBLOBs.setStatusHeartbeat("[{\"summaryMetric\":{\"totalRecordNumOfRead\""
                + ": \"10\"},"
                + "\"streamMetrics\":[{\"streamId\":\"stream1\"}]}]");
        groupHeartbeatEntityWithBLOBs.setReportTime(new Date());
        groupHeartbeatEntityWithBLOBs.setMetricHeartbeat("[{\"summaryMetric\":{\"totalRecordNumOfRead\""
                + ": \"10\"},"
                + "\"streamMetrics\":[{\"streamId\":\"stream1\"}]}]");
        Page<GroupHeartbeatEntityWithBLOBs> groupPage = new Page<>();
        groupPage.add(groupHeartbeatEntityWithBLOBs);
        groupPage.setTotal(1);
        given(groupHeartbeatEntityMapper.selectByKey(Mockito.anyString(),
                Mockito.anyString(), Mockito.anyString())).willReturn(groupHeartbeatEntityWithBLOBs);
        given(groupHeartbeatEntityMapper.selectHeartBeats(Mockito.anyString(),
                Mockito.anyString())).willReturn(groupPage);

        StreamHeartbeatEntityWithBLOBs streamHeartbeatEntityWithBLOBs = new StreamHeartbeatEntityWithBLOBs();
        streamHeartbeatEntityWithBLOBs.setComponent("Sort");
        streamHeartbeatEntityWithBLOBs.setInstance("127.0.0.1");
        streamHeartbeatEntityWithBLOBs.setInlongGroupId("group1");
        streamHeartbeatEntityWithBLOBs.setInlongStreamId("test_test");
        streamHeartbeatEntityWithBLOBs.setStatusHeartbeat("[{\"statue\":\"running\"}]");
        streamHeartbeatEntityWithBLOBs.setMetricHeartbeat("[{\"outMsg\":\"1\",\"inMsg\":2}]");
        streamHeartbeatEntityWithBLOBs.setReportTime(new Date());

        Page<StreamHeartbeatEntityWithBLOBs> streamPage = new Page<>();
        streamPage.add(streamHeartbeatEntityWithBLOBs);
        streamPage.setTotal(1);

        given(streamHeartbeatEntityMapper.selectByKey(Mockito.anyString(),
                Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
                .willReturn(streamHeartbeatEntityWithBLOBs);
        given(streamHeartbeatEntityMapper.selectHeartBeats(Mockito.anyString(),
                Mockito.anyString(), Mockito.anyString()))
                .willReturn(streamPage);
    }

    @Test
    public void testAddHeartBeat() throws Exception {

        HeartbeatReportRequest request = new HeartbeatReportRequest();
        request.setComponent("Sort");
        request.setInstance("127.0.0.1");
        request.setReportTimestamp(Instant.now().toEpochMilli());

        ComponentHeartBeat componentHeartBeat = new ComponentHeartBeat();
        componentHeartBeat.setMetricHeartbeat("{\"mem\":\"100\"}");
        componentHeartBeat.setStatusHeartbeat("{\"runningTime\":\"10h.35m\","
                + "\"status\":\"10h.35m\","
                + "\"groupIds\":\"group1,group2\"}");

        List<GroupHeartBeat> groupHeartBeats = new ArrayList<>();
        GroupHeartBeat groupHeartBeat = new GroupHeartBeat();
        groupHeartBeat.setInlongGroupId("group1");
        groupHeartBeat.setStatusHeartbeat("[{\"status\":\"running\",\"streamIds\":\"1,2,3,4\"}]");
        request.setGroupHeartBeats(groupHeartBeats);
        /*
         *
         */
        StreamHeartBeat streamHeartBeat = new StreamHeartBeat();
        streamHeartBeat.setMetricHeartbeat("[{\"summaryMetric\":{\"totalRecordNumOfRead\""
                + ": \"10\"},"
                + "\"streamMetrics\":[{\"streamId\":\"stream1\"}]}]");
        streamHeartBeat.setStatusHeartbeat("{}");
        streamHeartBeat.setInlongGroupId("group1");
        streamHeartBeat.setInlongStreamId("1");
        List<StreamHeartBeat> streamHeartBeats = new ArrayList<>();
        streamHeartBeats.add(streamHeartBeat);
        request.setStreamHeartBeats(streamHeartBeats);

        Assert.assertEquals("Success", heartbeatService.reportHeartbeatInfo(request));
    }

    @Test
    public void testQueryComponentHeartBeat() throws Exception {
        ComponentHeartBeatResponse response
                = heartbeatService.getComponentHeartbeatInfo("Sort", "127.0.0.1");
        Assert.assertEquals("127.0.0.1", response.getInstance());
    }

    @Test
    public void testQueryGroupHeartBeat() throws Exception {
        GroupHeartbeatResponse response
                = heartbeatService.getGroupHeartbeatInfo("Sort",
                "127.0.0.1", "group1");
        Assert.assertEquals("127.0.0.1", response.getInstance());
    }

    @Test
    public void testQueryStreamHeartBeat() throws Exception {
        StreamHeartBeatResponse response =
                heartbeatService.getStreamHeartbeatInfo("Sort",
                        "127.0.0.1", "group1", "stream1");
        Assert.assertEquals("127.0.0.1", response.getInstance());
    }

    @Test
    public void testQueryComponentHeartBeatPage() throws Exception {
        PageInfo<ComponentHeartBeatResponse> pageResponse
                = heartbeatService.getComponentHeartbeatInfos("Sort", 1,
                10);
        Assert.assertEquals(1, pageResponse.getTotal());
    }

    @Test
    public void testQueryGroupHeartBeatPage() throws Exception {
        PageInfo<GroupHeartbeatResponse> pageResponse
                = heartbeatService.getGroupHeartbeatInfos("Sort",
                "127.0.0.1", 1, 10);
        Assert.assertEquals(1, pageResponse.getTotal());
    }

    @Test
    public void testQueryStreamHeartBeatPage() throws Exception {
        PageInfo<StreamHeartBeatResponse> pageResponse =
                heartbeatService.getStreamHeartbeatInfos("Sort",
                        "127.0.0.1", "group1", 1, 10);
        Assert.assertEquals(1, pageResponse.getTotal());
    }
}
