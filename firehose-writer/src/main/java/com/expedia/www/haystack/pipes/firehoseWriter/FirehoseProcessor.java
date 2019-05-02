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
import com.expedia.www.haystack.pipes.commons.kafka.SpanProcessor;
import com.netflix.servo.util.VisibleForTesting;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

@Component
public class FirehoseProcessor implements SpanProcessor {
    final static Logger logger = LoggerFactory.getLogger(FirehoseProcessor.class);

    @VisibleForTesting
    static final String STARTUP_MESSAGE = "Instantiating FirehoseAction into stream name [%s]";
    @VisibleForTesting
    static final String PUT_RECORD_BATCH_ERROR_MSG = "putRecordBatch() could not put %d recordList after %d tries, but will continue trying...";

    private final FirehoseTimersAndCounters firehoseTimersAndCounters;
    private final Batch batch;
    private final Factory factory;
    private final FirehoseConfigurationProvider firehoseConfigurationProvider;
    private final S3Sender s3Sender;
    private TopicPartition topicPartition;
    private final List<BatchRecords> batchRecords;
    private final Semaphore semaphore;

    @Autowired
    FirehoseProcessor(Logger firehoseProcessorLogger,
                      FirehoseTimersAndCounters firehoseTimersAndCounters,
                      Supplier<Batch> batch,
                      Factory firehoseProcessorFactory,
                      FirehoseConfigurationProvider firehoseConfigurationProvider,
                      S3Sender s3Sender) {
        this.firehoseTimersAndCounters = firehoseTimersAndCounters;
        this.batch = batch.get();
        this.factory = firehoseProcessorFactory;
        this.firehoseConfigurationProvider = firehoseConfigurationProvider;
        this.s3Sender = s3Sender;
        firehoseProcessorLogger.info(String.format(STARTUP_MESSAGE, firehoseConfigurationProvider.streamname()));

        final int maxParallelism = firehoseConfigurationProvider.maxparallelismpershard();
        this.semaphore = factory.createSemaphore(maxParallelism);
        this.batchRecords = new ArrayList<>(maxParallelism);
    }

    @Override
    public Optional<Long> process(final ConsumerRecord<String, Span> record) {
        final Span span = record.value();
        firehoseTimersAndCounters.incrementRequestCounter();
        firehoseTimersAndCounters.recordSpanArrivalDelta(span);
        final List<Record> recordList = batch.getRecordList(span);

        if (recordList.isEmpty()) {
            return Optional.empty();
        }

        return processRecordsReinterruptingIfInterrupted(new BatchRecords(recordList, batchOffset(record.offset())));
    }

    private Optional<Long> processBatch(final BatchRecords batch) throws InterruptedException {
        semaphore.acquire();

        batchRecords.add(batch);

        final RetryCalculator retryCalculator =
                factory.createRetryCalculator(
                        firehoseConfigurationProvider.initialretrysleep(),
                        firehoseConfigurationProvider.maxretrysleep());

        final Sleeper sleeper = factory.createSleeper();
        final Callback callback = factory.createCallback(batch, semaphore);

        s3Sender.sendRecordsToS3(batch.records, retryCalculator, sleeper, 0, callback);

        // compute the offset that needs to be committed
        return anyOffsetToCommit();
    }

    @Override
    public void init(final TopicPartition topicPartition) {
        this.topicPartition = topicPartition;
        logger.info("Initializing the processor with topic partition {}", topicPartition);
    }

    @Override
    public void close() {
        logger.info("closing the processor with topic partition {}", topicPartition);
    }

    private long batchOffset(long offset) {
        return offset == 0 ? 0 : offset - 1;
    }

    private Optional<Long> anyOffsetToCommit() {
        Long offset = null;
        Iterator<BatchRecords> iterator = batchRecords.iterator();
        while (iterator.hasNext()) {
            final BatchRecords br = iterator.next();
            if (br.isCompleted.get()) {
                iterator.remove();
                offset = br.offset;
            } else {
                break;
            }
        }
        return Optional.ofNullable(offset);
    }

    private Optional<Long> processRecordsReinterruptingIfInterrupted(final BatchRecords batch) {
        try {
            return processBatch(batch);
        } catch (InterruptedException e) {
            factory.currentThread().interrupt();
        }
        return Optional.empty();
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

        Callback createCallback(final BatchRecords batch, final Semaphore semaphore) {
            return () -> {
                batch.isCompleted.set(true);
                semaphore.release();
            };
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

    static class BatchRecords {
        private final long offset;
        List<Record> records;
        AtomicBoolean isCompleted;
        BatchRecords(List<Record> records, long offset) {
            this.records = records;
            this.offset = offset;
            this.isCompleted = new AtomicBoolean(false);
        }
    }
}
