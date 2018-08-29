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

import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.kinesisfirehose.model.PutRecordBatchRequest;
import com.amazonaws.services.kinesisfirehose.model.PutRecordBatchResult;
import com.amazonaws.services.kinesisfirehose.model.Record;
import com.netflix.servo.monitor.Stopwatch;
import com.netflix.servo.util.VisibleForTesting;

import java.util.List;
import java.util.concurrent.Semaphore;

class FirehoseAsyncHandler implements AsyncHandler<PutRecordBatchRequest, PutRecordBatchResult> {
    @VisibleForTesting
    static final String RESULT_NULL = "PutRecordBatchResult is null; retrying %d recordList; retryCount=%d";

    @VisibleForTesting
    final Stopwatch stopwatch;
    @VisibleForTesting
    final PutRecordBatchRequest putRecordBatchRequest;
    @VisibleForTesting
    final int sleepMillis;
    @VisibleForTesting
    final int retryCount;
    @VisibleForTesting
    final List<Record> recordList;
    @VisibleForTesting
    final RetryCalculator retryCalculator;
    @VisibleForTesting
    final Sleeper sleeper;
    @VisibleForTesting
    final S3Sender s3Sender;
    @VisibleForTesting
    final Semaphore parallelismSemaphore;
    @VisibleForTesting
    final FirehoseCountersAndTimer firehoseCountersAndTimer;
    @VisibleForTesting
    final FailedRecordExtractor failedRecordExtractor;

    FirehoseAsyncHandler(S3Sender s3Sender,
                         Stopwatch stopwatch,
                         PutRecordBatchRequest putRecordBatchRequest,
                         int sleepMillis,
                         int retryCount,
                         List<Record> recordList,
                         RetryCalculator retryCalculator,
                         Sleeper sleeper,
                         Semaphore parallelismSemaphore,
                         FirehoseCountersAndTimer firehoseCountersAndTimer,
                         FailedRecordExtractor failedRecordExtractor) {
        this.stopwatch = stopwatch;
        this.putRecordBatchRequest = putRecordBatchRequest;
        this.sleepMillis = sleepMillis;
        this.retryCount = retryCount;
        this.recordList = recordList;
        this.retryCalculator = retryCalculator;
        this.sleeper = sleeper;
        this.s3Sender = s3Sender;
        this.parallelismSemaphore = parallelismSemaphore;
        this.firehoseCountersAndTimer = firehoseCountersAndTimer;
        this.failedRecordExtractor = failedRecordExtractor;
    }

    @Override
    public void onError(Exception exception) {
        s3Sender.onFirehoseCallback(stopwatch, putRecordBatchRequest, null, sleepMillis, retryCount, exception,
                firehoseCountersAndTimer);
        s3Sender.sendRecordsToS3(recordList, retryCalculator, sleeper, retryCount + 1, parallelismSemaphore);
    }

    @Override
    public void onSuccess(final PutRecordBatchRequest request, final PutRecordBatchResult result) {
        s3Sender.onFirehoseCallback(stopwatch, request, result, sleepMillis, retryCount, null,
                firehoseCountersAndTimer);
        if (s3Sender.areThereRecordsThatFirehoseHasNotProcessed(result.getFailedPutCount())) {
            final List<Record> failedRecords = failedRecordExtractor.extractFailedRecords(request, result, retryCount);
            s3Sender.sendRecordsToS3(
                    failedRecords, retryCalculator, sleeper, retryCount + 1, parallelismSemaphore);
        } else {
            parallelismSemaphore.release();
        }
    }

}
