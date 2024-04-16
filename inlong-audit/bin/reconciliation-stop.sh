#!/bin/bash
#
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

# this program kills the audit store
SCRIPT_DIR=$(
  # shellcheck disable=SC2164
  cd "$(dirname "$0")"
  pwd
)

BASE_DIR=$(dirname $SCRIPT_DIR)
PID_FILE="$BASE_DIR/bin/PID"
# Enter the root directory path
# shellcheck disable=SC2164
cd "$BASE_DIR"

check_server_is_running() {
    [[ ! -f $PID_FILE ]] && touch $PID_FILE
    PID=$(cat $PID_FILE)
    if [ "${PID}" = "" ]; then
      PID=0
      echo "server is not running ${PID}."
      return 0
    fi
    if [[ -d /proc/${PID}/cwd ]] && ls -ahl /proc/${PID}/cwd | grep -q "${BASE_DIR}";then
      return 1
    else
      echo "server is not running ${PID}."
      echo "" > $PID_FILE
      PID=0
      return 0
    fi
}
check_server_is_running
if [ $PID -gt 0 ]; then
  echo "server stop ${PID}!."
  kill -9 ${PID}
fi