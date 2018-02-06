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
import com.netflix.servo.monitor.Counter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
class Counters {
    private final Counter spanCounter;
    private final Counter successCounter;
    private final Counter failureCounter;

    @Autowired
    Counters(Counter spanCounter,
             Counter successCounter,
             Counter failureCounter) {
        this.spanCounter = spanCounter;
        this.successCounter = successCounter;
        this.failureCounter = failureCounter;
    }

    int countSuccessesAndFailures(PutRecordBatchRequest request, PutRecordBatchResult result) {
        final int recordCount = request.getRecords().size();
        final int failureCount = result == null ? recordCount : result.getFailedPutCount();
        final int successCount = recordCount - failureCount;
        successCounter.increment(successCount);
        failureCounter.increment(failureCount);
        return failureCount;
    }

    void incrementSpanCounter() {
        spanCounter.increment();
    }

}
