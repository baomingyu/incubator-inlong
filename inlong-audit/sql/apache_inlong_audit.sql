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

SET NAMES utf8;
SET FOREIGN_KEY_CHECKS = 0;
SET SQL_MODE = "NO_AUTO_VALUE_ON_ZERO";
SET time_zone = "+00:00";

-- ----------------------------
-- Database for InLong Audit
-- ----------------------------
CREATE DATABASE IF NOT EXISTS apache_inlong_audit;

USE apache_inlong_audit;

-- ----------------------------
-- Table structure for audit_data
-- ----------------------------
CREATE TABLE IF NOT EXISTS `audit_data`
(
    `id`               int(32)      NOT NULL PRIMARY KEY AUTO_INCREMENT COMMENT 'Incremental primary key',
    `ip`               varchar(32)  NOT NULL DEFAULT '' COMMENT 'Client IP',
    `docker_id`        varchar(100) NOT NULL DEFAULT '' COMMENT 'Client docker id',
    `thread_id`        varchar(50)  NOT NULL DEFAULT '' COMMENT 'Client thread id',
    `sdk_ts`           TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'SDK timestamp',
    `packet_id`        BIGINT       NOT NULL DEFAULT '0' COMMENT 'Packet id',
    `log_ts`           TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Log timestamp',
    `inlong_group_id`  varchar(100) NOT NULL DEFAULT '' COMMENT 'The target inlong group id',
    `inlong_stream_id` varchar(100) NOT NULL DEFAULT '' COMMENT 'The target inlong stream id',
    `audit_id`         varchar(100) NOT NULL DEFAULT '' COMMENT 'Audit id',
    `audit_tag`        varchar(100) DEFAULT '' COMMENT 'Audit tag',
    `count`            BIGINT       NOT NULL DEFAULT '0' COMMENT 'Message count',
    `size`             BIGINT       NOT NULL DEFAULT '0' COMMENT 'Message size',
    `delay`            BIGINT       NOT NULL DEFAULT '0' COMMENT 'Message delay count',
    `update_time`      timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Update time',
    INDEX group_stream_audit_id (`inlong_group_id`, `inlong_stream_id`, `audit_id`, `log_ts`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8 COMMENT ='Inlong audit data table';

-- ----------------------------
-- Table structure for audit_reconciliation_status
-- ----------------------------
CREATE TABLE IF NOT EXISTS `audit_reco_status`
(
    `topo_name`              varchar(256)  DEFAULT '' COMMENT 'Topology name',
    `group_id`               varchar(256)  NOT NULL DEFAULT '' COMMENT 'InLong group id',
    `stream_id`              varchar(256)  NOT NULL DEFAULT '' COMMENT 'InLong stream id',
    `partition_value`        varchar(20)   NOT NULL DEFAULT '' COMMENT 'Partition value yyyyMMddhhmmss',
    `partition_day`          varchar(10)   NOT NULL DEFAULT '' COMMENT 'Partition day yyyyMMdd',
    `us_task_id`             varchar(20)   DEFAULT '' COMMENT 'US task id',
    `audit_source_id`        varchar(16)   NOT NULL DEFAULT '' COMMENT 'Audit source id',
    `audit_sink_id`          varchar(16)   NOT NULL DEFAULT '' COMMENT 'Audit sink id',
    `audit_sink_abandon_id`  varchar(16)   NOT NULL DEFAULT '' COMMENT 'Audit sink abandon id',
    `audit_tag`              varchar(100)  DEFAULT '' COMMENT 'Audit tag',
    `audit_status`           varchar(8)    DEFAULT '' COMMENT 'Audit status',
    `audit_type`             varchar(16)   NOT NULL DEFAULT 'InLongAudit' COMMENT 'Audit type',
    `sink_type`              varchar(16)   NOT NULL DEFAULT '' COMMENT 'Sink type',
    `database`               varchar(32)   NOT NULL DEFAULT '' COMMENT 'Sink database',
    `table`                  varchar(32)   NOT NULL DEFAULT '' COMMENT 'Sink table',
    `cycle_unit`             varchar(2)    NOT NULL DEFAULT 'h' COMMENT 'Cycle unit',
    `cycle_number`           INTEGER       NOT NULL DEFAULT '1' COMMENT 'Cycle number',
    `receive_msg_id`         varchar(36)   NOT NULL DEFAULT ''  COMMENT 'Notify reconciliation pulsar msg Id',
    `send_msg_id`            varchar(36)   NOT NULL DEFAULT ''  COMMENT 'Notify outer pulsar msg Id after reconciliation',
    `partition_start_ms`     BIGINT        NOT NULL DEFAULT '0' COMMENT 'Partition begin time',
    `partition_end_ms`       BIGINT        NOT NULL DEFAULT '0' COMMENT 'Partition end time',
    `audit_source_cnt`       BIGINT        NOT NULL DEFAULT '0' COMMENT 'Audit source cnt',
    `audit_sink_cnt`         BIGINT        NOT NULL DEFAULT '0' COMMENT 'Audit sink cnt',
    `audit_sink_abandon_cnt` BIGINT        NOT NULL DEFAULT '0' COMMENT 'Audit sink abandon cnt',
    `retry_time`             INTEGER       NOT NULL DEFAULT '0' COMMENT 'Reconciliation retry time',
    `max_retry_time`         INTEGER       NOT NULL DEFAULT '12' COMMENT 'Max reconciliation retry time',
    `msg_handler_tag`        varchar(36)   NOT NULL DEFAULT '' COMMENT 'Reconciliation node tag',
    `check_ratio`            DOUBLE        NOT NULL DEFAULT '0.9995' COMMENT 'Audit check ratio',
    `create_time`            TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP  COMMENT 'Create datetime',
    `modify_time`            TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Modify datetime',
    `is_delete`              TINYINT       NOT NULL DEFAULT '0' COMMENT 'Is or not delete',
    INDEX group_stream_audit_id (`topo_name`, `group_id`, `stream_id`, `partition_value`, `partition_day`)
    ) ENGINE = InnoDB DEFAULT CHARSET = utf8 COMMENT ='InLong audit data reconciliation statue table'
   PARTITION BY LIST  COLUMNS(partition_day)
(PARTITION p_default VALUES IN ('default') ENGINE = InnoDB,
 PARTITION p_20240415 VALUES IN ('20240415') ENGINE = InnoDB,
 PARTITION p_20240416 VALUES IN ('20240416') ENGINE = InnoDB,
 PARTITION p_20240417 VALUES IN ('20240417') ENGINE = InnoDB,
 PARTITION p_20240418 VALUES IN ('20240418') ENGINE = InnoDB,
 PARTITION p_20240419 VALUES IN ('20240419') ENGINE = InnoDB,
    PARTITION p_20240420 VALUES IN ('20240420') ENGINE = InnoDB,
    PARTITION p_20240421 VALUES IN ('20240421') ENGINE = InnoDB,
    PARTITION p_20240422 VALUES IN ('20240422') ENGINE = InnoDB,
    PARTITION p_20240423 VALUES IN ('20240423') ENGINE = InnoDB,
    PARTITION p_20240424 VALUES IN ('20240424') ENGINE = InnoDB,
    PARTITION p_20240425 VALUES IN ('20240425') ENGINE = InnoDB,
    PARTITION p_20240426 VALUES IN ('20240426') ENGINE = InnoDB,
    PARTITION p_20240427 VALUES IN ('20240427') ENGINE = InnoDB,
    PARTITION p_20240428 VALUES IN ('20240428') ENGINE = InnoDB,
    PARTITION p_20240429 VALUES IN ('20240429') ENGINE = InnoDB,
    PARTITION p_20240430 VALUES IN ('20240430') ENGINE = InnoDB,
    PARTITION p_20240501 VALUES IN ('20240501') ENGINE = InnoDB,
    PARTITION p_20240502 VALUES IN ('20240502') ENGINE = InnoDB,
    PARTITION p_20240503 VALUES IN ('20240503') ENGINE = InnoDB,
    PARTITION p_20240504 VALUES IN ('20240504') ENGINE = InnoDB,
    PARTITION p_20240505 VALUES IN ('20240505') ENGINE = InnoDB,
    PARTITION p_20240506 VALUES IN ('20240506') ENGINE = InnoDB,
    PARTITION p_20240507 VALUES IN ('20240507') ENGINE = InnoDB,
    PARTITION p_20240508 VALUES IN ('20240508') ENGINE = InnoDB,
    PARTITION p_20240509 VALUES IN ('20240509') ENGINE = InnoDB,
    PARTITION p_20240510 VALUES IN ('20240510') ENGINE = InnoDB);

-- ----------------------------
-- Table structure for audit_reconciliation_human_notify
-- ----------------------------
CREATE TABLE IF NOT EXISTS `audit_reco_human_notify`
(
    `topo_name`              varchar(256)  DEFAULT '' COMMENT 'Topology name',
    `group_id`               varchar(256)  NOT NULL DEFAULT '' COMMENT 'InLong group id',
    `stream_id`              varchar(256)  NOT NULL DEFAULT '' COMMENT 'InLong stream id',
    `partition_value`        varchar(20)   NOT NULL DEFAULT '' COMMENT 'Partition value yyyyMMddhhmmss',
    `partition_day`          varchar(10)   NOT NULL DEFAULT '' COMMENT 'Partition day yyyyMMdd',
    `us_task_id`             varchar(20)   NOT NULL DEFAULT '' COMMENT 'US task id',
    `sink_database`               varchar(32)   DEFAULT '' COMMENT 'Sink database',
    `sink_table`                  varchar(32)   DEFAULT '' COMMENT 'Sink table',
    `send_msg_id`            varchar(36)   NOT NULL DEFAULT ''  COMMENT 'Notify outer pulsar msg Id after reconciliation',
    `create_time`            TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP  COMMENT 'Create datetime',
    `modify_time`            TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Modify datetime',
    `is_delete`              TINYINT       NOT NULL DEFAULT '0' COMMENT 'Is or not delete',
    INDEX group_stream_partition_value (`group_id`, `stream_id`, `partition_value`, `partition_day`)
    ) ENGINE = InnoDB DEFAULT CHARSET = utf8 COMMENT ='InLong audit data reconciliation human notify water table'
    PARTITION BY LIST  COLUMNS(partition_day)
(PARTITION p_default VALUES IN ('default') ENGINE = InnoDB,
    PARTITION p_20240424 VALUES IN ('20240424') ENGINE = InnoDB,
    PARTITION p_20240425 VALUES IN ('20240425') ENGINE = InnoDB,
    PARTITION p_20240426 VALUES IN ('20240426') ENGINE = InnoDB,
    PARTITION p_20240427 VALUES IN ('20240427') ENGINE = InnoDB,
    PARTITION p_20240428 VALUES IN ('20240428') ENGINE = InnoDB,
    PARTITION p_20240429 VALUES IN ('20240429') ENGINE = InnoDB,
    PARTITION p_20240430 VALUES IN ('20240430') ENGINE = InnoDB,
    PARTITION p_20240501 VALUES IN ('20240501') ENGINE = InnoDB,
    PARTITION p_20240502 VALUES IN ('20240502') ENGINE = InnoDB,
    PARTITION p_20240503 VALUES IN ('20240503') ENGINE = InnoDB,
    PARTITION p_20240504 VALUES IN ('20240504') ENGINE = InnoDB,
    PARTITION p_20240505 VALUES IN ('20240505') ENGINE = InnoDB,
    PARTITION p_20240506 VALUES IN ('20240506') ENGINE = InnoDB,
    PARTITION p_20240507 VALUES IN ('20240507') ENGINE = InnoDB,
    PARTITION p_20240508 VALUES IN ('20240508') ENGINE = InnoDB,
    PARTITION p_20240509 VALUES IN ('20240509') ENGINE = InnoDB,
    PARTITION p_20240510 VALUES IN ('20240510') ENGINE = InnoDB);