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
package com.expedia.www.haystack.pipes.firehoseWriter;

import org.slf4j.Logger;

class UnexpectedExceptionLogger {
    static final String UNEXPECTED_EXCEPTION_MSG = "Unexpected exception received from AWS Firehose, will retry all records";
    private final Logger logger;

    UnexpectedExceptionLogger(Logger logger) {
        this.logger = logger;
    }

    void logError(Exception exception) {
        logger.error(UNEXPECTED_EXCEPTION_MSG, exception);
    }
}
