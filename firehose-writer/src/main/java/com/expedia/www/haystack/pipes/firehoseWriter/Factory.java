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

class Factory {
    static class SleeperImpl implements FirehoseProcessor.Sleeper {
        @Override
        public void sleep(long millis) throws InterruptedException {
            Thread.sleep(millis);
        }
    }

    PutRecordBatchRequest createPutRecordBatchRequest(String streamName, List<Record> records) {
        final PutRecordBatchRequest putRecordBatchRequest = new PutRecordBatchRequest();
        putRecordBatchRequest.setDeliveryStreamName(streamName);
        putRecordBatchRequest.setRecords(records);
        return putRecordBatchRequest;
    }

    FirehoseProcessor.Sleeper createSleeper() {
        return new SleeperImpl();
    }

    Runtime getRuntime() {
        return Runtime.getRuntime();
    }

    Thread createShutdownHook(FirehoseProcessor firehoseProcessor) {
        return new Thread(new ShutdownHook(firehoseProcessor));
    }

    Thread currentThread() {
        return Thread.currentThread();
    }

    RetryCalculator createRetryCalculator(int initialRetrySleep, int maxRetrySleep) {
        return new RetryCalculator(initialRetrySleep, maxRetrySleep);
    }

    BlockingQueueEntry createBlockingQueueEntry(Stopwatch stopwatch,
                                                RetryCalculator retryCalculator,
                                                PutRecordBatchRequest putRecordBatchRequest,
                                                Batch batch,
                                                Future<PutRecordBatchResult> futurePutRecordBatchResult) {
        return new BlockingQueueEntry(
                stopwatch, retryCalculator, putRecordBatchRequest, batch, futurePutRecordBatchResult);
    }
}
