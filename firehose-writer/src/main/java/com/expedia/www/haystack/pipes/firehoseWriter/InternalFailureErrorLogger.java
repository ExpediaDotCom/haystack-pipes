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

import com.amazonaws.services.kinesisfirehose.model.PutRecordBatchRequest;
import com.amazonaws.services.kinesisfirehose.model.PutRecordBatchResult;
import org.slf4j.Logger;

class InternalFailureErrorLogger {
    private final Logger logger;

    InternalFailureErrorLogger(Logger logger) {
        this.logger = logger;
    }

    void logError(PutRecordBatchRequest request,
                  PutRecordBatchResult result,
                  int retryCount) {
        logger.error(String.format(FailedRecordExtractor.INTERNAL_FAILURE_MSG,
                request.getRecords().size(), retryCount, result.getSdkResponseMetadata().getRequestId()));
    }
}
