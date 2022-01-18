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

package org.apache.inlong.manager.dao.entity;

import java.io.Serializable;
import java.util.Date;
import lombok.Data;

@Data
public class DBCollectorDetailTaskEntity implements Serializable {

    private static final long serialVersionUID = 1L;
    private Integer id;
    private Integer mainId;
    private Integer type;
    private String timeVar;
    private Integer dbType;
    private String ip;
    private Integer port;
    private String dbName;
    private String user;
    private String password;
    private String sqlStatement;
    private Integer offset;
    private Integer totalLimit;
    private Integer onceLimit;
    private Integer timeLimit;
    private Integer retryTimes;
    private String groupId;
    private String streamId;
    private Integer state;
    private Date createTime;
    private Date modifyTime;
}