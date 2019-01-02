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
package com.expedia.www.haystack.pipes.secretDetector.actions;

import com.netflix.servo.util.VisibleForTesting;
import org.slf4j.Logger;

public class FromAddressExceptionLogger {
    @VisibleForTesting
    static final String FROM_ADDRESS_EXCEPTION_MSG = "Problem using eMail configurations: from [%s]";

    private final Logger logger;

    public FromAddressExceptionLogger(Logger logger) {
        this.logger = logger;
    }

    void logError(String sFrom, Exception exception) {
        final String message = String.format(FROM_ADDRESS_EXCEPTION_MSG, sFrom);
        logger.error(message, exception);
    }

}
