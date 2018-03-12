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

import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehose;
import com.amazonaws.services.kinesisfirehose.model.PutRecordBatchRequest;
import com.amazonaws.services.kinesisfirehose.model.PutRecordBatchResult;
import com.amazonaws.services.kinesisfirehose.model.Record;
import com.expedia.open.tracing.Span;
import com.netflix.servo.monitor.Stopwatch;
import com.netflix.servo.monitor.Timer;
import com.netflix.servo.util.VisibleForTesting;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

@Component
public class FirehoseProcessor implements Processor<String, Span> {
    @VisibleForTesting
    static final String STARTUP_MESSAGE = "Instantiating FirehoseAction into stream name [%s]";
    @VisibleForTesting
    static final String PUT_RECORD_BATCH_WARN_MSG = "putRecordBatch() failed; retryCount=%d";
    @VisibleForTesting
    static final String PUT_RECORD_BATCH_ERROR_MSG = "putRecordBatch() could not put %d records after %d tries";

    private final Logger logger;
    private final Counters counters;
    private final Timer putBatchRequestTimer;
    private final Batch batch;
    private final AmazonKinesisFirehose amazonKinesisFirehose;
    private final Factory factory;
    private final FirehoseConfigurationProvider firehoseConfigurationProvider;

    @Autowired
    FirehoseProcessor(Logger firehoseProcessorLogger,
                      Counters counters,
                      Timer putBatchRequestTimer,
                      Batch batch,
                      AmazonKinesisFirehose amazonKinesisFirehose,
                      Factory firehoseProcessorFactory,
                      FirehoseConfigurationProvider firehoseConfigurationProvider) {
        this.logger = firehoseProcessorLogger;
        this.counters = counters;
        this.putBatchRequestTimer = putBatchRequestTimer;
        this.batch = batch;
        this.amazonKinesisFirehose = amazonKinesisFirehose;
        this.factory = firehoseProcessorFactory;
        this.firehoseConfigurationProvider = firehoseConfigurationProvider;

        this.logger.info(String.format(STARTUP_MESSAGE, firehoseConfigurationProvider.streamname()));
    }

    @Override
    public void init(ProcessorContext context) {
        final Thread shutdownHook = factory.createShutdownHook(this);
        final Runtime runtime = factory.getRuntime();
        runtime.addShutdownHook(shutdownHook);
    }

    @Override
    public void process(String key, Span span) {
        counters.incrementSpanCounter();
        final List<Record> records = batch.getRecordList(span);
        sendRecordsToS3(records);
    }

    @Override
    public void close() {
        final List<Record> records = batch.getRecordListForShutdown();
        sendRecordsToS3(records);
    }

    private void sendRecordsToS3(List<Record> records) {
        int retryCount = 0;
        int failureCount;
        if (!records.isEmpty()) {
            final String streamName = firehoseConfigurationProvider.streamname();
            final AtomicReference<Exception> exceptionForErrorLogging = new AtomicReference<>(null);
            final int retryCountLimitFromConfiguration = Integer.parseInt(firehoseConfigurationProvider.retrycount());
            do {
                final PutRecordBatchRequest request = factory.createPutRecordBatchRequest(streamName, records);
                PutRecordBatchResult result = null;
                final Stopwatch stopwatch = putBatchRequestTimer.start();
                try {
                    factory.createSleeper().sleep(((1 << retryCount) * 1000) - 1000); // 0s,1s,3s,7s,15s,31s...
                    result = amazonKinesisFirehose.putRecordBatch(request);
                    exceptionForErrorLogging.set(null); // success! clear the exception if this was a retry
                } catch (Exception exception) {
                    final String errorMsg = String.format(PUT_RECORD_BATCH_WARN_MSG, retryCount++);
                    logger.warn(errorMsg, exception);
                    exceptionForErrorLogging.compareAndSet(null, exception); // save the first exception
                    continue;
                } finally {
                    stopwatch.stop();
                    failureCount = counters.countSuccessesAndFailures(request, result);
                }
                if (result == null || result.getFailedPutCount() > 0) {
                    records = batch.extractFailedRecords(request, result, retryCount++);
                } else {
                    retryCount = retryCountLimitFromConfiguration; // All records successfully put
                }
            } while (retryCount < retryCountLimitFromConfiguration);
            if (failureCount != 0 || exceptionForErrorLogging.get() != null) {
                final String msg = String.format(PUT_RECORD_BATCH_ERROR_MSG, failureCount, retryCount);
                logger.error(msg, exceptionForErrorLogging.get());
            }
        }
    }

    @Override
    public void punctuate(long timestamp) {
        // There is nothing to do; FirehoseProcessor does not schedule itself with the context provided in init() above.
    }

    /**
     * This interface exists so that unit tests can verify the appropriate amount of sleeping during retries,
     * and so that those unit tests run as fast as possible (i.e. without really sleeping).
     */
    interface Sleeper {
        void sleep(long millis) throws InterruptedException;
    }

    static class Factory {
        class SleeperImpl implements Sleeper {
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

        Sleeper createSleeper() {
            return new SleeperImpl();
        }

        Runtime getRuntime() {
            return Runtime.getRuntime();
        }

        Thread createShutdownHook(FirehoseProcessor firehoseProcessor) {
            final ShutdownHook shutdownHook = new ShutdownHook(firehoseProcessor);
            return new Thread(shutdownHook);
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