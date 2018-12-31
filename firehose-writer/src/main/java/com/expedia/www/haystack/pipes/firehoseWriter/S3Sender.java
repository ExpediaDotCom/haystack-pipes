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

import com.amazonaws.SdkClientException;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseAsync;
import com.amazonaws.services.kinesisfirehose.model.PutRecordBatchRequest;
import com.amazonaws.services.kinesisfirehose.model.PutRecordBatchResult;
import com.amazonaws.services.kinesisfirehose.model.Record;
import com.netflix.servo.monitor.Stopwatch;
import com.netflix.servo.util.VisibleForTesting;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.net.SocketTimeoutException;
import java.util.List;
import java.util.concurrent.Semaphore;

import static com.expedia.www.haystack.pipes.firehoseWriter.FirehoseProcessor.PUT_RECORD_BATCH_ERROR_MSG;

@Component
public class S3Sender {
    @VisibleForTesting
    private final FirehoseConfigurationProvider firehoseConfigurationProvider;
    private final Factory factory;
    private final FirehoseTimersAndCounters firehoseTimersAndCounters;
    private final AmazonKinesisFirehoseAsync amazonKinesisFirehoseAsync;
    private final Logger logger;
    private final FailedRecordExtractor failedRecordExtractor;
    private final UnexpectedExceptionLogger unexpectedExceptionLogger;

    @Autowired
    public S3Sender(FirehoseConfigurationProvider firehoseConfigurationProvider,
                    Factory factory,
                    FirehoseTimersAndCounters firehoseTimersAndCounters,
                    AmazonKinesisFirehoseAsync amazonKinesisFirehoseAsync,
                    Logger s3SenderLogger,
                    FailedRecordExtractor failedRecordExtractor,
                    UnexpectedExceptionLogger unexpectedExceptionLogger) {
        this.firehoseConfigurationProvider = firehoseConfigurationProvider;
        this.factory = factory;
        this.firehoseTimersAndCounters = firehoseTimersAndCounters;
        this.amazonKinesisFirehoseAsync = amazonKinesisFirehoseAsync;
        this.logger = s3SenderLogger;
        this.failedRecordExtractor = failedRecordExtractor;
        this.unexpectedExceptionLogger = unexpectedExceptionLogger;
    }

    void sendRecordsToS3(final List<Record> records,
                         final RetryCalculator retryCalculator,
                         final Sleeper sleeper,
                         final int retryCount,
                         final Semaphore parallelism) {
        final String streamName = firehoseConfigurationProvider.streamname();
        final PutRecordBatchRequest request = factory.createPutRecordBatchRequest(streamName, records);
        final Stopwatch stopwatch = firehoseTimersAndCounters.startTimer();
        final int sleepMillis = retryCalculator.calculateSleepMillis();
        boolean isInterrupted = false;

        // If the main thread is interrupted while sleeping, the best behavior at this point in the code is to try
        // to send the recordList to S3 (because not sending them could result in lost spans in Athena) but still stop
        // execution; see https://www.ibm.com/developerworks/library/j-jtp05236/index.html. So we'll remember that an
        // interrupt was requested and then reinterrupt after sending our request.
        try {
            factory.createSleeper().sleep(sleepMillis);
        } catch (InterruptedException e) {
            isInterrupted = true;
        } finally {
            amazonKinesisFirehoseAsync.putRecordBatchAsync(request,
                    factory.createFirehoseAsyncHandler(this, stopwatch, request, sleepMillis, retryCount,
                            records, retryCalculator, sleeper, parallelism, firehoseTimersAndCounters,
                            failedRecordExtractor));
        }
        if(isInterrupted) {
            factory.currentThread().interrupt();
        }
    }

    void onFirehoseCallback(final Stopwatch stopwatch,
                            final PutRecordBatchRequest request,
                            final PutRecordBatchResult result,
                            final int sleepMillis,
                            final int retryCount,
                            Exception exception,
                            FirehoseTimersAndCounters firehoseTimersAndCounters) {
        stopwatch.stop();
        if(exception != null) {
            if (exception instanceof SdkClientException && exception.getCause() instanceof SocketTimeoutException) {
                firehoseTimersAndCounters.incrementSocketTimeoutCounter();
            } else {
                unexpectedExceptionLogger.logError(exception);
            }
        }
        int failureCount = firehoseTimersAndCounters.countSuccessesAndFailures(request, result);

        final int maxRetrySleep = firehoseConfigurationProvider.maxretrysleep();
        if (shouldLogErrorMessage(failureCount, maxRetrySleep, sleepMillis)) {
            logger.error(String.format(PUT_RECORD_BATCH_ERROR_MSG, failureCount, retryCount));
        }
    }

    @VisibleForTesting
    boolean shouldLogErrorMessage(int failureCount, int maxRetrySleep, int sleepMillis) {
        return hasSleepMillisReachedItsLimit(maxRetrySleep, sleepMillis)
                && (areThereRecordsThatFirehoseHasNotProcessed(failureCount));
    }

    private boolean hasSleepMillisReachedItsLimit(int maxRetrySleep, int sleepMillis) {
        return sleepMillis == maxRetrySleep;
    }

    boolean areThereRecordsThatFirehoseHasNotProcessed(int failureCount) {
        return failureCount > 0;
    }

    static class Factory extends FactoryBase {
        FirehoseAsyncHandler createFirehoseAsyncHandler(S3Sender s3Sender,
                                                        Stopwatch stopwatch,
                                                        PutRecordBatchRequest request,
                                                        int sleepMillis,
                                                        int retryCount,
                                                        List<Record> records,
                                                        RetryCalculator retryCalculator,
                                                        Sleeper sleeper,
                                                        Semaphore parallelism,
                                                        FirehoseTimersAndCounters firehoseTimersAndCounters,
                                                        FailedRecordExtractor failedRecordExtractor) {
            return new FirehoseAsyncHandler(s3Sender, stopwatch, request, sleepMillis, retryCount, records,
                    retryCalculator, sleeper, parallelism, firehoseTimersAndCounters, failedRecordExtractor);
        }

        PutRecordBatchRequest createPutRecordBatchRequest(String streamName, List<Record> records) {
            return new PutRecordBatchRequest().withDeliveryStreamName(streamName).withRecords(records);
        }

    }

}
