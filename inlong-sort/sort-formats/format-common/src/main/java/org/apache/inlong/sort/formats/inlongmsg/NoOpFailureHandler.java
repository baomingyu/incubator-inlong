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

package org.apache.inlong.sort.formats.inlongmsg;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of {@link FailureHandler} that just throws the exception out.
 */
public class NoOpFailureHandler implements FailureHandler {

    private static final Logger LOG = LoggerFactory.getLogger(NoOpFailureHandler.class);

    @Override
    public void onParsingMsgFailure(Object msg, Exception t) throws Exception {
        LOG.error("Could not properly serialize msg=[{}].", msg, t);
        throw t;
    }

    @Override
    public void onParsingHeadFailure(String attribute, Exception exception) throws Exception {
        LOG.error("Cannot properly parse the head {}", attribute, exception);
        throw exception;
    }

    @Override
    public void onParsingBodyFailure(InLongMsgHead head, byte[] body, Exception exception) throws Exception {
        LOG.error("Cannot properly parse the head: {}, the body: {}.", head, new String(body), exception);
        throw exception;
    }

    @Override
    public void onConvertingRowFailure(InLongMsgHead head, InLongMsgBody body, Exception exception) throws Exception {
        LOG.error("Cannot properly convert the InLongMsg ({}, {})", head, body, exception);
        throw exception;
    }

    @Override
    public boolean isIgnoreFailure() {
        return false;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        return o != null && getClass() == o.getClass();
    }
}
