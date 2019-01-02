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

import java.time.Clock;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import com.expedia.www.haystack.pipes.commons.Timers;
import com.netflix.servo.util.VisibleForTesting;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseAsync;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseAsyncClientBuilder;
import com.expedia.www.haystack.metrics.MetricObjects;
import com.expedia.www.haystack.pipes.commons.health.HealthController;
import com.expedia.www.haystack.pipes.commons.health.UpdateHealthStatusFile;
import com.expedia.www.haystack.pipes.commons.kafka.KafkaConfigurationProvider;
import com.expedia.www.haystack.pipes.commons.kafka.KafkaStreamStarter;
import com.expedia.www.haystack.pipes.commons.serialization.SerdeFactory;
import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.util.JsonFormat.Printer;
import com.netflix.servo.monitor.Counter;
import com.netflix.servo.monitor.Timer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.expedia.www.haystack.pipes.commons.CommonConstants.SPAN_ARRIVAL_TIMER_NAME;
import static com.expedia.www.haystack.pipes.commons.CommonConstants.SUBSYSTEM;
import static com.expedia.www.haystack.pipes.firehoseWriter.Constants.APPLICATION;

/**
 * Spring configuration class. The FirehoseIsActiveControllerTest.testMainWithMockObjects() unit test creates a new
 * AnnotationConfigApplicationContext that loads this class and instantiates all Spring beans defined in it,
 * so less than 100% unit test coverage of this class indicates a Spring configuration bean that is not used.
 */
@Configuration
@ComponentScan(basePackageClasses = SpringConfig.class)
class SpringConfig {
    @VisibleForTesting static final String SPAN_COUNTER_NAME = "REQUEST";
    @VisibleForTesting static final String SUCCESS_COUNTER_NAME = "SUCCESS";
    @VisibleForTesting static final String FAILURE_COUNTER_NAME = "FAILURE";
    @VisibleForTesting static final String EXCEPTION_COUNTER_NAME = "EXCEPTION";
    @VisibleForTesting static final String THROTTLED_COUNTER_NAME = "THROTTLED";
    @VisibleForTesting static final String SOCKET_TIMEOUT_COUNTER_NAME = "SOCKET_TIMEOUT";
    @VisibleForTesting static final String PUT_BATCH_REQUEST_TIMER_NAME = "PUT_BATCH_REQUEST";

    private final MetricObjects metricObjects;

    /**
     * @param metricObjects provided by a static inner class that is loaded before this SpringConfig class
     * @see MetricObjectsSpringConfig
     */
    @Autowired
    SpringConfig(MetricObjects metricObjects) {
        this.metricObjects = metricObjects;
    }

    // Beans with unit tests ///////////////////////////////////////////////////////////////////////////////////////////
    @Bean
    Counter spanCounter() {
        return metricObjects.createAndRegisterResettingCounter(SUBSYSTEM, APPLICATION,
                FirehoseProcessor.class.getName(), SPAN_COUNTER_NAME);
    }

    @Bean
    Counter successCounter() {
        return metricObjects.createAndRegisterResettingCounter(SUBSYSTEM, APPLICATION,
                FirehoseProcessor.class.getName(), SUCCESS_COUNTER_NAME);
    }

    @Bean
    Counter failureCounter() {
        return metricObjects.createAndRegisterResettingCounter(SUBSYSTEM, APPLICATION,
                FirehoseProcessor.class.getName(), FAILURE_COUNTER_NAME);
    }

    @Bean
    Counter exceptionCounter() {
        return metricObjects.createAndRegisterResettingCounter(SUBSYSTEM, APPLICATION,
                FirehoseProcessor.class.getName(), EXCEPTION_COUNTER_NAME);
    }

    @Bean
    Counter throttledCounter() {
        return metricObjects.createAndRegisterResettingCounter(SUBSYSTEM, APPLICATION,
                Batch.class.getName(), THROTTLED_COUNTER_NAME);
    }

    @Bean
    Counter socketTimeoutCounter() {
        return metricObjects.createAndRegisterResettingCounter(SUBSYSTEM, APPLICATION,
                S3Sender.class.getName(), SOCKET_TIMEOUT_COUNTER_NAME);
    }

    @Bean
    Timer putBatchRequestTimer() {
        return metricObjects.createAndRegisterBasicTimer(SUBSYSTEM, APPLICATION,
                FirehoseProcessor.class.getName(), PUT_BATCH_REQUEST_TIMER_NAME, TimeUnit.MILLISECONDS);
    }

    @Bean
    Timer spanArrivalTimer() {
        return metricObjects.createAndRegisterBasicTimer(SUBSYSTEM, APPLICATION,
                FirehoseProcessor.class.getName(), SPAN_ARRIVAL_TIMER_NAME, TimeUnit.MILLISECONDS);
    }

    @Bean
    @Autowired
    KafkaStreamStarter kafkaStreamStarter(final HealthController healthController) {
        return new KafkaStreamStarter(ProtobufToFirehoseProducer.class, APPLICATION, healthController);
    }

    @Bean
    HealthController healthController() {
        final HealthController healthController = new HealthController();
        healthController.addListener(new UpdateHealthStatusFile("/app/isHealthy")); // TODO should come from config
        return healthController;
    }

    @Bean
    Logger firehoseProcessorLogger() {
        return LoggerFactory.getLogger(FirehoseProcessor.class);
    }

    @Bean
    Logger s3SenderLogger() {
        return LoggerFactory.getLogger(S3Sender.class);
    }

    @Bean
    Logger protobufToFirehoseProducerLogger() {
        return LoggerFactory.getLogger(ProtobufToFirehoseProducer.class);
    }

    @Bean
    Logger firehoseIsActiveControllerLogger() {
        return LoggerFactory.getLogger(FirehoseIsActiveController.class);
    }

    @Bean
    Logger batchLogger() {
        return LoggerFactory.getLogger(Batch.class);
    }

    @Bean
    Logger failedRecordExtractorLogger() {
        return LoggerFactory.getLogger(FailedRecordExtractor.class);
    }

    @Bean
    Logger internalFailureErrorLoggerLogger() {
        return LoggerFactory.getLogger(InternalFailureErrorLogger.class);
    }

    @Bean
    Logger unexpectedExceptionLoggerLogger() {
        return LoggerFactory.getLogger(UnexpectedExceptionLogger.class);
    }

    @Bean
    @Autowired
    FailedRecordExtractor failedRecordExtractor(Logger failedRecordExtractorLogger,
                                                Counter throttledCounter,
                                                InternalFailureErrorLogger internalFailureErrorLogger) {
        return new FailedRecordExtractor(failedRecordExtractorLogger, throttledCounter, internalFailureErrorLogger);
    }

    @Bean
    @Autowired
    EndpointConfiguration endpointConfiguration(String url,
                                                String signingregion) {
        return new EndpointConfiguration(url, signingregion);
    }

    @Bean
    @Autowired
    String url(FirehoseConfigurationProvider firehoseConfigurationProvider) {
        return firehoseConfigurationProvider.url();
    }

    @Bean
    @Autowired
    String signingregion(FirehoseConfigurationProvider firehoseConfigurationProvider) {
        return firehoseConfigurationProvider.signingregion();
    }

    @Bean
    ClientConfiguration clientConfiguration() {
        final ClientConfiguration clientConfiguration = new ClientConfiguration();
        clientConfiguration.setUseGzip(true);
        return clientConfiguration;
    }

    // Beans without unit tests ////////////////////////////////////////////////////////////////////////////////////////
    @Bean
    @Autowired
    InternalFailureErrorLogger internalFailureErrorLogger(Logger internalFailureErrorLoggerLogger) {
        return new InternalFailureErrorLogger(internalFailureErrorLoggerLogger);
    }

    @Bean
    @Autowired
    UnexpectedExceptionLogger unexpectedExceptionLogger(Logger unexpectedExceptionLoggerLogger) {
        return new UnexpectedExceptionLogger(unexpectedExceptionLoggerLogger);
    }

    @Bean
    @Autowired
    AmazonKinesisFirehoseAsync amazonKinesisFirehoseAsync(EndpointConfiguration endpointConfiguration,
                                                          ClientConfiguration clientConfiguration) {
        return AmazonKinesisFirehoseAsyncClientBuilder
                .standard()
                .withEndpointConfiguration(endpointConfiguration)
                .withClientConfiguration(clientConfiguration)
                .build();
    }

    @Bean
    Supplier<FirehoseCollector> firehoseCollector(FirehoseConfigurationProvider configurationProvider) {
        return () -> configurationProvider.usestringbuffering() ?
                new FirehoseByteArrayCollector(configurationProvider.maxbatchinterval()) : new FirehoseRecordBufferCollector();
    }

    @Bean
    SerdeFactory serdeFactory() {
        return new SerdeFactory();
    }

    @Bean
    FirehoseIsActiveController.Factory firehoseIsActiveControllerFactory() {
        return new FirehoseIsActiveController.Factory();
    }

    @Bean
    FirehoseConfigurationProvider firehoseConfigurationProvider() {
        return new FirehoseConfigurationProvider();
    }

    @Bean
    FirehoseProcessor.Factory firehoseProcessorFactory() {
        return new FirehoseProcessor.Factory();
    }

    @Bean
    S3Sender.Factory s3SenderFactory() {
        return new S3Sender.Factory();
    }

    @Bean
    Clock clock()  {
        return Clock.systemUTC();
    }

    @Bean
    @Autowired
    Timers timers(Timer putBatchRequestTimer,
                  Timer spanArrivalTimer) {
        return new Timers(putBatchRequestTimer, spanArrivalTimer);
    }

    @Bean
    @Autowired
    FirehoseTimersAndCounters counters(Clock clock,
                                      Timers timers,
                                      Counter spanCounter,
                                      Counter successCounter,
                                      Counter failureCounter,
                                      Counter exceptionCounter,
                                      Counter socketTimeoutCounter) {
        return new FirehoseTimersAndCounters(clock, timers,
                spanCounter, successCounter, failureCounter, exceptionCounter, socketTimeoutCounter);
    }

    @Bean
    @Autowired
    Supplier<Batch> batch(Printer printer,
                          Supplier<FirehoseCollector> firehoseCollector,
                          Logger batchLogger) {
        return () -> new Batch(printer, firehoseCollector, batchLogger);
    }

    @Bean
    @Autowired
    FirehoseProcessorSupplier firehoseProcessorSupplier(Logger firehoseProcessorLogger,
                                                        FirehoseTimersAndCounters firehoseTimersAndCounters,
                                                        Supplier<Batch> batch,
                                                        FirehoseProcessor.Factory firehoseProcessorFactory,
                                                        FirehoseConfigurationProvider firehoseConfigurationProvider,
                                                        S3Sender s3Sender) {
        return new FirehoseProcessorSupplier(firehoseProcessorLogger, firehoseTimersAndCounters, batch,
                firehoseProcessorFactory, firehoseConfigurationProvider, s3Sender);
    }

    @Bean
    KafkaConfigurationProvider kafkaConfigurationProvider() {
        return new KafkaConfigurationProvider();
    }

    @Bean
    @Autowired
    ProtobufToFirehoseProducer protobufToFirehoseProducer(KafkaStreamStarter kafkaStreamStarter,
                                                          SerdeFactory serdeFactory,
                                                          FirehoseProcessorSupplier firehoseProcessorSupplier,
                                                          KafkaConfigurationProvider kafkaConfigurationProvider) {
        return new ProtobufToFirehoseProducer(
                kafkaStreamStarter, serdeFactory, firehoseProcessorSupplier, kafkaConfigurationProvider);
    }

    @Bean
    Printer printer() {
        return JsonFormat.printer().omittingInsignificantWhitespace();
    }

    /**
     * Spring loads this static inner class before loading the SpringConfig outer class so that its bean is available to
     * the outer class constructor.
     *
     * @see SpringConfig
     */
    @Configuration
    static class MetricObjectsSpringConfig {
        @Bean
        public MetricObjects metricObjects() {
            return new MetricObjects();
        }
    }
}

