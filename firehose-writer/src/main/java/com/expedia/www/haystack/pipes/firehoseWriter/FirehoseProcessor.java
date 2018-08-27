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

import com.amazonaws.services.kinesisfirehose.model.Record;
import com.expedia.open.tracing.Span;
import com.netflix.servo.util.VisibleForTesting;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.function.Supplier;

@Component
public class FirehoseProcessor extends AbstractProcessor<String, Span> {
    @VisibleForTesting
    static final String STARTUP_MESSAGE = "Instantiating FirehoseAction into stream name [%s]";
    @VisibleForTesting
    static final String PUT_RECORD_BATCH_ERROR_MSG = "putRecordBatch() could not put %d recordList after %d tries, but will continue trying...";

    private final FirehoseCountersAndTimer firehoseCountersAndTimer;
    private final Batch batch;
    private final Factory factory;
    private final FirehoseConfigurationProvider firehoseConfigurationProvider;
    private final Semaphore parallelismSemaphore;
    private final S3Sender s3Sender;

    @Autowired
    FirehoseProcessor(Logger firehoseProcessorLogger,
                      FirehoseCountersAndTimer firehoseCountersAndTimer,
                      Supplier<Batch> batch,
                      Factory firehoseProcessorFactory,
                      FirehoseConfigurationProvider firehoseConfigurationProvider,
                      S3Sender s3Sender) {
        this.firehoseCountersAndTimer = firehoseCountersAndTimer;
        this.batch = batch.get();
        this.factory = firehoseProcessorFactory;
        this.firehoseConfigurationProvider = firehoseConfigurationProvider;
        this.s3Sender = s3Sender;
        firehoseProcessorLogger.info(String.format(STARTUP_MESSAGE, firehoseConfigurationProvider.streamname()));
        this.parallelismSemaphore = factory.createSemaphore(firehoseConfigurationProvider.maxparallelismpershard());
    }

    @Override
    public void process(String key, Span span) {
        firehoseCountersAndTimer.incrementRequestCounter();
        firehoseCountersAndTimer.recordSpanArrivalDelta(span);
        final List<Record> records = batch.getRecordList(span);
        processRecordsReinterruptingIfInterrupted(records);
    }

    private void processRecords(List<Record> records) throws InterruptedException {
        if (!records.isEmpty()) {
            parallelismSemaphore.acquire();
            final RetryCalculator retryCalculator = factory.createRetryCalculator(
                    firehoseConfigurationProvider.initialretrysleep(),
                    firehoseConfigurationProvider.maxretrysleep());
            final Sleeper sleeper = factory.createSleeper();
            s3Sender.sendRecordsToS3(records, retryCalculator, sleeper, 0, parallelismSemaphore);
        }
    }

    @Override
    public void close() {
        final List<Record> records = batch.getRecordListForShutdown();
        processRecordsReinterruptingIfInterrupted(records);
    }

    private void processRecordsReinterruptingIfInterrupted(List<Record> records) {
        try {
            processRecords(records);
        } catch (InterruptedException e) {
            factory.currentThread().interrupt();
        }
    }

    static class Factory extends FactoryBase {
        RetryCalculator createRetryCalculator(int initialRetrySleep, int maxRetrySleep) {
            return new RetryCalculator(initialRetrySleep, maxRetrySleep);
        }

        Runtime getRuntime() {
            return Runtime.getRuntime();
        }

        Thread createShutdownHook(FirehoseProcessor firehoseProcessor) {
            return new Thread(new ShutdownHook(firehoseProcessor));
        }

        Semaphore createSemaphore(int maxParallelismPerShare) {
            return new Semaphore(maxParallelismPerShare);
        }
    }

    static class ShutdownHook implements Runnable {
        private final FirehoseProcessor firehoseProcessor;

        ShutdownHook(FirehoseProcessor firehoseProcessor) {
            this.firehoseProcessor = firehoseProcessor;
        }

        @Override
        public void run() {
            firehoseProcessor.close();
        }
    }

}
