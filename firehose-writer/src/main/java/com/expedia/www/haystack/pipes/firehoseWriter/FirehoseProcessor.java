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
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.function.Supplier;

@Component
public class FirehoseProcessor implements Processor<String, Span> {
    @VisibleForTesting
    static final String STARTUP_MESSAGE = "Instantiating FirehoseAction into stream name [%s]";

    private final FirehoseCountersAndTimer firehoseCountersAndTimer;
    private final Batch batch;
    private final FirehoseConfigurationProvider firehoseConfigurationProvider;
    private final Factory factory;
    private final S3Sender s3Sender;

    @Autowired
    FirehoseProcessor(Logger firehoseProcessorLogger,
                      FirehoseCountersAndTimer firehoseCountersAndTimer,
                      Supplier<Batch> batch,
                      FirehoseConfigurationProvider firehoseConfigurationProvider,
                      Factory factory,
                      S3Sender s3Sender) {
        firehoseProcessorLogger.info(String.format(STARTUP_MESSAGE, firehoseConfigurationProvider.streamname()));
        this.firehoseCountersAndTimer = firehoseCountersAndTimer;
        this.batch = batch.get();
        this.firehoseConfigurationProvider = firehoseConfigurationProvider;
        this.factory = factory;
        this.s3Sender = s3Sender;
    }

    @Override
    public void init(ProcessorContext context) {
        /* do nothing */
    }

    @Override
    public void process(String key, Span span) {
        firehoseCountersAndTimer.incrementRequestCounter();
        firehoseCountersAndTimer.recordSpanArrivalDelta(span);
        final List<Record> records = batch.getRecordList(span);
        try {
            final RetryCalculator retryCalculator = factory.createRetryCalculator(
                    firehoseConfigurationProvider.initialretrysleep(), firehoseConfigurationProvider.maxretrysleep());
            s3Sender.sendRecordsToS3(records, retryCalculator);
        } catch (InterruptedException e) {
            factory.currentThread().interrupt();
        }
    }

    @Override
    public void close() {
        s3Sender.close();
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

}
