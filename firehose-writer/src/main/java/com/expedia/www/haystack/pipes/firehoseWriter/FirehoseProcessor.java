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
    static final String PUT_RECORD_BATCH_ERROR_MSG = "putRecordBatch() could not put %d records after %d tries, but will continue trying...";

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

    private class RetryCalculator {
        final int initialRetrySleep;
        final int maxRetrySleep;
        int boundedTryCount; // never increments more than one step past the value that causes the exponential backoff
                             // time calculation to exceed maxRetrySleep; this avoids problems for > 31 tries.
        private RetryCalculator(int initialRetrySleep, int maxRetrySleep) {
            this.initialRetrySleep = initialRetrySleep;
            this.maxRetrySleep = maxRetrySleep;
        }

        /**
         * Calculates the number of milliseconds to sleep. The first time this method is called, it returns 0.
         * The second time it returns 1 * initialRetrySleep, the third time it returns 2 * initialRetrySleep,
         * the fourth time it returns 4 * initialRetrySleep, the fifth time it returns 8 * initialRetrySleep, etc.
         * But the returned value is bounded by maxRetrySleep; it will never return a number larger than maxRetrySleep.
         * @return msec to sleep
         */
        private int calculateSleepMillis() {
            final int sleepMillisPerTryCount = (1 << (boundedTryCount - 1)) * initialRetrySleep;
            final int sleepMillis;
            if(sleepMillisPerTryCount > maxRetrySleep) {
                sleepMillis = maxRetrySleep;
            } else {
                sleepMillis = sleepMillisPerTryCount;
                ++boundedTryCount;
            }
            return Math.min(sleepMillis, maxRetrySleep);
        }
    }

    private void sendRecordsToS3(List<Record> records) {
        int retryCount = 0;
        int failureCount;
        if (!records.isEmpty()) {
            final String streamName = firehoseConfigurationProvider.streamname();
            final AtomicReference<Exception> exceptionForErrorLogging = new AtomicReference<>(null);
            final int maxRetrySleep = firehoseConfigurationProvider.maxretrysleep();
            final RetryCalculator retryCalculator = new RetryCalculator(
                    firehoseConfigurationProvider.initialretrysleep(), maxRetrySleep);
            boolean allRecordsPutSuccessfully = false;
            final Sleeper sleeper = factory.createSleeper();
            do {
                final PutRecordBatchRequest request = factory.createPutRecordBatchRequest(streamName, records);
                PutRecordBatchResult result = null;
                final Stopwatch stopwatch = putBatchRequestTimer.start();
                final int sleepMillis = retryCalculator.calculateSleepMillis();
                try {
                    sleeper.sleep(sleepMillis);
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
                if (result == null || areThereRecordsThatFirehoseHasNotProcessed(result.getFailedPutCount())) {
                    records = batch.extractFailedRecords(request, result, retryCount++);
                } else {
                    allRecordsPutSuccessfully = true;
                }
                if (shouldLogErrorMessage(failureCount, exceptionForErrorLogging, maxRetrySleep, sleepMillis)) {
                    final String msg = String.format(PUT_RECORD_BATCH_ERROR_MSG, failureCount, retryCount);
                    System.out.println(msg);
                    logger.error(msg, exceptionForErrorLogging.get());
                }
            } while (!allRecordsPutSuccessfully);
        }
    }

    @VisibleForTesting
    boolean shouldLogErrorMessage(int failureCount,
                                  AtomicReference<Exception> exceptionForErrorLogging,
                                  int maxRetrySleep,
                                  int sleepMillis) {
        return hasSleepMillisReachedItsLimit(maxRetrySleep, sleepMillis)
                && (areThereRecordsThatFirehoseHasNotProcessed(failureCount)
                 || wasAnExceptionThrownByFirehose(exceptionForErrorLogging));
    }

    private boolean hasSleepMillisReachedItsLimit(int maxRetrySleep, int sleepMillis) {
        return sleepMillis == maxRetrySleep;
    }

    private boolean areThereRecordsThatFirehoseHasNotProcessed(int failureCount) {
        return failureCount > 0;
    }

    private boolean wasAnExceptionThrownByFirehose(AtomicReference<Exception> exceptionForErrorLogging) {
        return exceptionForErrorLogging.get() != null;
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
