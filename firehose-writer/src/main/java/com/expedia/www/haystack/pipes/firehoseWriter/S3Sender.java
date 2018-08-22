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

import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseAsync;
import com.amazonaws.services.kinesisfirehose.model.PutRecordBatchRequest;
import com.amazonaws.services.kinesisfirehose.model.PutRecordBatchResult;
import com.amazonaws.services.kinesisfirehose.model.Record;
import com.expedia.www.haystack.pipes.firehoseWriter.FirehoseProcessor.Sleeper;
import com.netflix.servo.monitor.Stopwatch;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Future;
import java.util.function.Supplier;

@Component
public class S3Sender {
    private final Sleeper sleeper;
    private final Factory factory;
    private final FirehoseConfigurationProvider firehoseConfigurationProvider;
    private final String streamName;
    private final FirehoseCountersAndTimer firehoseCountersAndTimer;
    private final AmazonKinesisFirehoseAsync amazonKinesisFirehoseAsync;
    private final Batch batch;
    private final ArrayBlockingQueue<BlockingQueueEntry> arrayBlockingQueue;

    @Autowired
    public S3Sender(Supplier<Batch> batchSupplier,
                    Sleeper sleeper,
                    Factory firehoseProcessorFactory,
                    FirehoseConfigurationProvider firehoseConfigurationProvider,
                    FirehoseCountersAndTimer firehoseCountersAndTimer,
                    AmazonKinesisFirehoseAsync amazonKinesisFirehoseAsync,
                    ArrayBlockingQueue<BlockingQueueEntry> arrayBlockingQueue) {
        this.batch = batchSupplier.get();
        this.sleeper = sleeper;
        this.factory = firehoseProcessorFactory;
        this.firehoseConfigurationProvider = firehoseConfigurationProvider;
        this.streamName = firehoseConfigurationProvider.streamname();
        this.firehoseCountersAndTimer = firehoseCountersAndTimer;
        this.amazonKinesisFirehoseAsync = amazonKinesisFirehoseAsync;
        this.arrayBlockingQueue = arrayBlockingQueue;
    }

    /**
     * Sends records to S3
     * @param recordList records to send
     * @param retryCalculator RetryCalculator to use; pass in a new RetryCalculator for the first putRecordBatchRequest, but for
     *                        retries send in the RetryCalculator used for the initial try
     */
    void sendRecordsToS3(List<Record> recordList, RetryCalculator retryCalculator) throws InterruptedException {
        if (!recordList.isEmpty()) {
            sleeper.sleep(retryCalculator.calculateSleepMillis());
            final PutRecordBatchRequest putRecordBatchRequest =
                    factory.createPutRecordBatchRequest(streamName, recordList);
            final Stopwatch stopwatch = firehoseCountersAndTimer.startTimer();
            final Future<PutRecordBatchResult> recordBatchResultFuture =
                    amazonKinesisFirehoseAsync.putRecordBatchAsync(putRecordBatchRequest);
            final BlockingQueueEntry blockingQueueEntry = factory.createBlockingQueueEntry(
                    stopwatch, retryCalculator, putRecordBatchRequest, batch, recordBatchResultFuture);
            arrayBlockingQueue.put(blockingQueueEntry);
        }
    }

    void close() {
        final List<Record> recordList = batch.getRecordListForShutdown();
        try {
            final RetryCalculator retryCalculator = factory.createRetryCalculator(
                    firehoseConfigurationProvider.initialretrysleep(), firehoseConfigurationProvider.maxretrysleep());
            sendRecordsToS3(recordList, retryCalculator);
        } catch (InterruptedException e) {
            factory.currentThread().interrupt();
        }
    }

}
