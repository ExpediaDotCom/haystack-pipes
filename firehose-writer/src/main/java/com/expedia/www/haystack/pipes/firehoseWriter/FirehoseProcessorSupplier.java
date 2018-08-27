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

import com.expedia.open.tracing.Span;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.function.Supplier;

@Component
public class FirehoseProcessorSupplier implements ProcessorSupplier<String, Span> {
    private final Logger firehoseProcessorLogger;
    private final FirehoseCountersAndTimer firehoseCountersAndTimer;
    private final Supplier<Batch> batch;
    private final FirehoseProcessor.Factory firehoseProcessorFactory;
    private final FirehoseConfigurationProvider firehoseConfigurationProvider;
    private final S3Sender s3Sender;

    @Autowired
    public FirehoseProcessorSupplier(Logger firehoseProcessorLogger,
                                     FirehoseCountersAndTimer firehoseCountersAndTimer,
                                     Supplier<Batch> batch,
                                     FirehoseProcessor.Factory firehoseProcessorFactory,
                                     FirehoseConfigurationProvider firehoseConfigurationProvider, S3Sender s3Sender) {
        this.firehoseProcessorLogger = firehoseProcessorLogger;
        this.firehoseCountersAndTimer = firehoseCountersAndTimer;
        this.batch = batch;
        this.firehoseProcessorFactory = firehoseProcessorFactory;
        this.firehoseConfigurationProvider = firehoseConfigurationProvider;
        this.s3Sender = s3Sender;
    }

    @Override
    public Processor<String, Span> get() {
        return new FirehoseProcessor(firehoseProcessorLogger, firehoseCountersAndTimer, batch,
                firehoseProcessorFactory, firehoseConfigurationProvider, s3Sender);
    }
}
