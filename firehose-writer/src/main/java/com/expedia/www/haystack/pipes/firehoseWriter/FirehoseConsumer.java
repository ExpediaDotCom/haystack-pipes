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
import com.netflix.servo.util.VisibleForTesting;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;

@Component
public class FirehoseConsumer implements Runnable {
    @VisibleForTesting
    static final String EXECUTION_EXCEPTION_ERROR_MSG = "putRecordBatch() saw an ExceptionException, but will continue trying...";
    @VisibleForTesting
    static final String PUT_RECORD_BATCH_ERROR_MSG = "putRecordBatch() could not put %d records after %d tries, but will continue trying...";
    @VisibleForTesting
    static final String STARTUP_MSG = "Starting FirehoseConsumer thread";

    @VisibleForTesting
    final S3Sender s3Sender;
    @VisibleForTesting
    final Logger logger;
    @VisibleForTesting
    final FirehoseCountersAndTimer firehoseCountersAndTimer;
    @VisibleForTesting
    final ArrayBlockingQueue<BlockingQueueEntry> arrayBlockingQueue;

    @Autowired
    public FirehoseConsumer(S3Sender s3Sender,
                            Logger firehoseConsumerLogger,
                            FirehoseCountersAndTimer firehoseCountersAndTimer,
                            ArrayBlockingQueue<BlockingQueueEntry> arrayBlockingQueue) {
        this.s3Sender = s3Sender;
        this.logger = firehoseConsumerLogger;
        this.firehoseCountersAndTimer = firehoseCountersAndTimer;
        this.arrayBlockingQueue = arrayBlockingQueue;
    }

    @Override
    public void run() {
        logger.info(STARTUP_MSG);
        PutRecordBatchResult putRecordBatchResult;
        BlockingQueueEntry blockingQueueEntry;
        boolean isRunning = true;
        while (isRunning) {
            putRecordBatchResult = null;
            blockingQueueEntry = null;
            try {
                blockingQueueEntry = arrayBlockingQueue.take();
                putRecordBatchResult = blockingQueueEntry.getFuturePutRecordBatchResult().get();
            } catch (InterruptedException e) { // take()/get(): blockingQueueEntry ?= null, putRecordBatchResult == null
                isRunning = false;
            } catch (ExecutionException e) {   // get()       : blockingQueueEntry != null, putRecordBatchResult == null
                firehoseCountersAndTimer.incrementExceptionCounter();
                logger.error(EXECUTION_EXCEPTION_ERROR_MSG, e);
            } finally {
                if (blockingQueueEntry != null) {
                    blockingQueueEntry.getStopwatch().stop();
                    final PutRecordBatchRequest putRecordBatchRequest = blockingQueueEntry.getPutRecordBatchRequest();
                    firehoseCountersAndTimer.countSuccessesAndFailures(putRecordBatchRequest, putRecordBatchResult);
                    final List<Record> failedRecords = blockingQueueEntry.extractFailedRecords(putRecordBatchResult);
                    if (!failedRecords.isEmpty()) {
                        final RetryCalculator retryCalculator = blockingQueueEntry.getRetryCalculator();
                        if(retryCalculator.isTryCountBeyondLimit()) {
                            logger.error(PUT_RECORD_BATCH_ERROR_MSG,
                                    failedRecords.size(), retryCalculator.getUnboundedTryCount());
                        }
                        try {
                            s3Sender.sendRecordsToS3(failedRecords, retryCalculator);
                        } catch (InterruptedException e) {
                            isRunning = false;
                        }
                    }
                }
            }
        }
    }
}
