/*
 * Copyright 2018 Expedia, Inc.
 *
 *       Licensed under the Apache License, Version 2.0 (the "License");
 *       you may not use this file except in compliance with the License.
 *       You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *       Unless required by applicable law or agreed to in writing, software
 *       distributed under the License is distributed on an "AS IS" BASIS,
 *       WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *       See the License for the specific language governing permissions and
 *       limitations under the License.
 *
 */
package com.expedia.www.haystack.pipes.httpPoster;

import com.expedia.open.tracing.Span;
import com.expedia.www.haystack.pipes.commons.CommonConstants;
import org.slf4j.Logger;

import static com.expedia.www.haystack.pipes.commons.CommonConstants.PROTOBUF_ERROR_MSG;

class InvalidProtocolBufferExceptionLogger {
    private final Logger logger;

    InvalidProtocolBufferExceptionLogger(Logger logger) {
        this.logger = logger;
    }

    void logError(Span span, Exception exception) {
        logger.error(String.format(PROTOBUF_ERROR_MSG, span.toString(), exception.getMessage()), exception);
    }

}
