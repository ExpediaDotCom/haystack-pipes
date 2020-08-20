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

import com.expedia.www.haystack.pipes.commons.kafka.SpanProcessor;
import com.expedia.www.haystack.pipes.commons.kafka.SpanProcessorSupplier;
import com.expedia.www.haystack.pipes.commons.kafka.config.FirehoseConfig;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.function.Supplier;

@Component
public class FirehoseProcessorSupplier implements SpanProcessorSupplier {
    private final Logger firehoseProcessorLogger;
    private final FirehoseTimersAndCounters firehoseTimersAndCounters;
    private final Supplier<Batch> batch;
    private final FirehoseProcessor.Factory firehoseProcessorFactory;
    private final FirehoseConfig firehoseConfigurationProvider;
    private final S3Sender s3Sender;

    @Autowired
    public FirehoseProcessorSupplier(Logger firehoseProcessorLogger,
                                     FirehoseTimersAndCounters firehoseTimersAndCounters,
                                     Supplier<Batch> batch,
                                     FirehoseProcessor.Factory firehoseProcessorFactory,
                                     FirehoseConfig firehoseConfigurationProvider, S3Sender s3Sender) {
        this.firehoseProcessorLogger = firehoseProcessorLogger;
        this.firehoseTimersAndCounters = firehoseTimersAndCounters;
        this.batch = batch;
        this.firehoseProcessorFactory = firehoseProcessorFactory;
        this.firehoseConfigurationProvider = firehoseConfigurationProvider;
        this.s3Sender = s3Sender;
    }

    @Override
    public SpanProcessor get() {
        return new FirehoseProcessor(firehoseProcessorLogger, firehoseTimersAndCounters, batch,
                firehoseProcessorFactory, firehoseConfigurationProvider, s3Sender);
    }
}
