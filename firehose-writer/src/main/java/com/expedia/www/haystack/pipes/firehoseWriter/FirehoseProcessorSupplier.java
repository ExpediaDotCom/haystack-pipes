package com.expedia.www.haystack.pipes.firehoseWriter;

import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseAsync;
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
    private final AmazonKinesisFirehoseAsync amazonKinesisFirehoseAsync;
    private final FirehoseProcessor.Factory firehoseProcessorFactory;
    private final FirehoseConfigurationProvider firehoseConfigurationProvider;

    @Autowired
    public FirehoseProcessorSupplier(Logger firehoseProcessorLogger,
                                     FirehoseCountersAndTimer firehoseCountersAndTimer,
                                     Supplier<Batch> batch,
                                     AmazonKinesisFirehoseAsync amazonKinesisFirehoseAsync,
                                     FirehoseProcessor.Factory firehoseProcessorFactory,
                                     FirehoseConfigurationProvider firehoseConfigurationProvider) {
        this.firehoseProcessorLogger = firehoseProcessorLogger;
        this.firehoseCountersAndTimer = firehoseCountersAndTimer;
        this.batch = batch;
        this.amazonKinesisFirehoseAsync = amazonKinesisFirehoseAsync;
        this.firehoseProcessorFactory = firehoseProcessorFactory;
        this.firehoseConfigurationProvider = firehoseConfigurationProvider;
    }

    @Override
    public Processor<String, Span> get() {
        return new FirehoseProcessor(firehoseProcessorLogger, firehoseCountersAndTimer, batch,
                amazonKinesisFirehoseAsync, firehoseProcessorFactory, firehoseConfigurationProvider);
    }
}
