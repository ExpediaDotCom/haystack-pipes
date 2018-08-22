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
import com.amazonaws.services.kinesisfirehose.model.Record;
import com.netflix.servo.monitor.Stopwatch;

import java.util.List;
import java.util.concurrent.Future;

class BlockingQueueEntry {
    private final Stopwatch stopwatch;
    private final RetryCalculator retryCalculator;
    private final PutRecordBatchRequest putRecordBatchRequest;
    private final Batch batch;
    private final Future<PutRecordBatchResult> futurePutRecordBatchResult;

    BlockingQueueEntry(Stopwatch stopwatch,
                       RetryCalculator retryCalculator,
                       PutRecordBatchRequest putRecordBatchRequest,
                       Batch batch,
                       Future<PutRecordBatchResult> futurePutRecordBatchResult) {
        this.stopwatch = stopwatch;
        this.retryCalculator = retryCalculator;
        this.putRecordBatchRequest = putRecordBatchRequest;
        this.batch = batch;
        this.futurePutRecordBatchResult = futurePutRecordBatchResult;
    }

    Stopwatch getStopwatch() {
        return stopwatch;
    }

    RetryCalculator getRetryCalculator() {
        return retryCalculator;
    }

    PutRecordBatchRequest getPutRecordBatchRequest() {
        return putRecordBatchRequest;
    }

    Batch getBatch() {
        return batch;
    }

    Future<PutRecordBatchResult> getFuturePutRecordBatchResult() {
        return futurePutRecordBatchResult;
    }

    /**
     * Extracts failed records from a putRecordBatch attempt. This method requires to putRecordBatchResult as an
     * argument; this argument should always be exactly the object obtained by calling get() on the
     * futurePutRecordBatchResult instance variable; requiring it as an argument avoids having to deal with the
     * exceptions that get() can throw, and clearly there can be no attempt to extract failed records if the result
     * of the call has not yet been received. Typically the result will contain information about which records need
     * to be retried, but if a null is passed to this method, all records in the request will be returned for retry.
     *
     * @param putRecordBatchResult the result of the AWS call
     * @return the records that should be retried
     */
    List<Record> extractFailedRecords(PutRecordBatchResult putRecordBatchResult) {
        final List<Record> records;
        if (putRecordBatchResult != null) {
            records = batch.extractFailedRecords(putRecordBatchRequest,
                    putRecordBatchResult, retryCalculator.getUnboundedTryCount());
        } else {
            records = putRecordBatchRequest.getRecords();
        }
        return records;
    }

}
