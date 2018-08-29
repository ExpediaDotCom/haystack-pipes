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
import com.expedia.www.haystack.pipes.commons.CountersAndTimer;
import com.netflix.servo.monitor.Counter;
import com.netflix.servo.monitor.Timer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.Clock;

@Component
class FirehoseCountersAndTimer extends CountersAndTimer {
    private static final int SUCCESS_COUNTER_INDEX = 0;
    private static final int FAILURE_COUNTER_INDEX = 1;
    private static final int EXCEPTION_COUNTER_INDEX = 2;
    private static final int SOCKET_TIMEOUT_COUNTER_INDEX = 3;

    @Autowired
    FirehoseCountersAndTimer(Clock clock,
                             Timer putBatchRequestTimer,
                             Timer spanArrivalTimer,
                             Counter spanCounter,
                             Counter successCounter,
                             Counter failureCounter,
                             Counter exceptionCounter,
                             Counter socketTimeoutCounter) {
        super(clock,
                putBatchRequestTimer, spanArrivalTimer,
                spanCounter, successCounter, failureCounter, exceptionCounter, socketTimeoutCounter);
    }

    int countSuccessesAndFailures(PutRecordBatchRequest request, PutRecordBatchResult result) {
        final int recordCount = request.getRecords().size();
        final int failureCount = result == null ? recordCount : result.getFailedPutCount();
        final int successCount = recordCount - failureCount;
        incrementCounter(SUCCESS_COUNTER_INDEX, successCount);
        incrementCounter(FAILURE_COUNTER_INDEX, failureCount);
        return failureCount;
    }

    void incrementExceptionCounter() {
        incrementCounter(EXCEPTION_COUNTER_INDEX);
    }

    void incrementSocketTimeoutCounter() {
        incrementCounter(SOCKET_TIMEOUT_COUNTER_INDEX);
    }

}
